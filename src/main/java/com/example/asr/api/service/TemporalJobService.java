package com.example.asr.api.service;

import com.example.asr.api.dto.CreateJobResponse;
import com.example.asr.api.dto.RetryAsrRequest;
import com.example.asr.api.dto.JobStatusResponse;
import com.example.asr.api.entity.JobRecord;
import com.example.asr.api.repo.JobRecordRepository;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowOptions;
import io.temporal.client.WorkflowStub;
import io.temporal.api.enums.v1.WorkflowExecutionStatus;
import io.temporal.workflow.QueryMethod;
import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

@Service
public class TemporalJobService {

    private final WorkflowClient workflowClient;
    private final JobRecordRepository repository;
    private final StorageService storageService;
    private final String workflowTaskQueue;
    private final String mediaTaskQueue;
    private final String asrTaskQueue;

    public TemporalJobService(WorkflowClient workflowClient,
                              JobRecordRepository repository,
                              StorageService storageService,
                              @Value("${app.temporal.workflow-task-queue}") String workflowTaskQueue,
                              @Value("${app.temporal.media-task-queue}") String mediaTaskQueue,
                              @Value("${app.temporal.asr-task-queue}") String asrTaskQueue) {
        this.workflowClient = workflowClient;
        this.repository = repository;
        this.storageService = storageService;
        this.workflowTaskQueue = workflowTaskQueue;
        this.mediaTaskQueue = mediaTaskQueue;
        this.asrTaskQueue = asrTaskQueue;
    }

    public CreateJobResponse createJob(MultipartFile file, String targetBucket) throws Exception {
        UUID jobId = UUID.randomUUID();
        String objectKey = storageService.uploadOriginal(jobId, file, targetBucket);
        String workflowId = "asr-job-" + jobId;

        JobRecord jobRecord = new JobRecord();
        jobRecord.setId(jobId);
        jobRecord.setWorkflowId(workflowId);
        jobRecord.setStatus("CREATED");
        jobRecord.setCurrentStep("UPLOADED");
        jobRecord.setProgressPercent(0);
        jobRecord.setInputObjectKey(objectKey);
        jobRecord.setTargetBucket(targetBucket != null && !targetBucket.isBlank() ? targetBucket : storageService.getDefaultBucket());
        repository.save(jobRecord);

        WorkflowOptions options = WorkflowOptions.newBuilder()
                .setTaskQueue(workflowTaskQueue)
                .setWorkflowId(workflowId)
                .build();

        AsrWorkflow workflow = workflowClient.newWorkflowStub(AsrWorkflow.class, options);
        WorkflowClient.start(workflow::start, new StartAsrRequest(jobId.toString(), objectKey, mediaTaskQueue, asrTaskQueue, jobRecord.getTargetBucket()));

        return new CreateJobResponse(jobId, workflowId, "STARTED", Instant.now());
    }

    public CreateJobResponse startJobFromExternalStorage(UUID jobId, String objectKey) {
        return startJobFromExternalStorage(jobId, objectKey, null);
    }

    public CreateJobResponse startJobFromExternalStorage(UUID jobId, String objectKey, String targetBucket) {
        String bucket = targetBucket;
        String finalObjectKey = objectKey;

        // Auto-parse bucket if not provided and path contains a slash
        if ((bucket == null || bucket.isBlank()) && objectKey.contains("/")) {
            int firstSlash = objectKey.indexOf('/');
            bucket = objectKey.substring(0, firstSlash);
            finalObjectKey = objectKey.substring(firstSlash + 1);
        }

        // Fallback to default if bucket still empty
        if (bucket == null || bucket.isBlank()) {
            bucket = storageService.getDefaultBucket();
        }

        String workflowId = "asr-job-" + jobId;

        // Check if record exists, if not create new
        JobRecord jobRecord = repository.findById(jobId).orElse(new JobRecord());
        jobRecord.setId(jobId);
        jobRecord.setWorkflowId(workflowId);
        jobRecord.setStatus("CREATED");
        jobRecord.setCurrentStep("EXTERNAL_UPLOAD");
        jobRecord.setProgressPercent(0);
        jobRecord.setInputObjectKey(finalObjectKey);
        jobRecord.setTargetBucket(bucket);
        repository.save(jobRecord);

        WorkflowOptions options = WorkflowOptions.newBuilder()
                .setTaskQueue(workflowTaskQueue)
                .setWorkflowId(workflowId)
                .build();

        AsrWorkflow workflow = workflowClient.newWorkflowStub(AsrWorkflow.class, options);
        WorkflowClient.start(workflow::start, new StartAsrRequest(jobId.toString(), finalObjectKey, mediaTaskQueue, asrTaskQueue, bucket));

        return new CreateJobResponse(jobId, workflowId, "STARTED", Instant.now());
    }

    public JobStatusResponse getJob(UUID jobId) {
        JobRecord record = repository.findById(jobId)
                .orElseThrow(() -> new IllegalArgumentException("Job not found: " + jobId));

        try {
            AsrWorkflow workflow = workflowClient.newWorkflowStub(AsrWorkflow.class, record.getWorkflowId());
            WorkflowJobStatus live = workflow.getStatus();

            record.setStatus(live.status());
            record.setCurrentStep(live.currentStep());
            record.setProgressPercent(live.progressPercent());
            record.setTotalChunks(live.totalChunks());
            record.setCompletedChunks(live.completedChunks());
            record.setTranscriptObjectKey(live.transcriptObjectKey());
            record.setErrorMessage(live.errorMessage());
            repository.save(record);
        } catch (RuntimeException ignored) {
            // Use last persisted view when workflow query is temporarily unavailable.
        }

        return map(record);
    }

    public void cancelJob(UUID jobId) {
        JobRecord record = repository.findById(jobId)
                .orElseThrow(() -> new IllegalArgumentException("Job not found: " + jobId));
        WorkflowStub.fromTyped(workflowClient.newWorkflowStub(AsrWorkflow.class, record.getWorkflowId())).cancel();
        record.setStatus("CANCELLING");
        repository.save(record);
    }

    public CreateJobResponse retryAsr(RetryAsrRequest request) {
        UUID jobId = UUID.fromString(request.jobId());
        String workflowId = "asr-job-" + jobId + "-" + request.workflowID();
        String path = request.path();

        // Similar logic to startJobFromExternalStorage but with explicit workflowId
        String bucket = null;
        String finalObjectKey = path;

        if (path.contains("/")) {
            int firstSlash = path.indexOf('/');
            bucket = path.substring(0, firstSlash);
            finalObjectKey = path.substring(firstSlash + 1);
        }

        if (bucket == null || bucket.isBlank()) {
            bucket = storageService.getDefaultBucket();
        }

        JobRecord jobRecord = repository.findById(jobId).orElse(new JobRecord());
        jobRecord.setId(jobId);
        jobRecord.setWorkflowId(workflowId);
        jobRecord.setStatus("PROCESSING");
        jobRecord.setCurrentStep("RETRYING");
        jobRecord.setProgressPercent(0);
        jobRecord.setCompletedChunks(0);
        jobRecord.setTotalChunks(0);
        jobRecord.setTranscriptObjectKey(null);
        jobRecord.setErrorMessage(null);
        jobRecord.setInputObjectKey(finalObjectKey);
        jobRecord.setTargetBucket(bucket);
        repository.save(jobRecord);

        WorkflowOptions options = WorkflowOptions.newBuilder()
                .setTaskQueue(workflowTaskQueue)
                .setWorkflowId(workflowId)
                .build();

        AsrWorkflow workflow = workflowClient.newWorkflowStub(AsrWorkflow.class, options);
        WorkflowClient.start(workflow::start, new StartAsrRequest(jobId.toString(), finalObjectKey, mediaTaskQueue, asrTaskQueue, bucket));

        return new CreateJobResponse(jobId, workflowId, "STARTED", Instant.now());
    }

    public CreateJobResponse regenerateAnalysis(UUID jobId, String workflowIdSuffix) {
        JobRecord record = repository.findById(jobId)
                .orElseThrow(() -> new IllegalArgumentException("Job not found: " + jobId));

        if (record.getTranscriptObjectKey() == null) {
            throw new IllegalStateException("Cannot regenerate analysis: Transcript not yet generated for job " + jobId);
        }

        String fullWorkflowId = "analysis-regen-" + jobId + "-" + workflowIdSuffix;

        WorkflowOptions options = WorkflowOptions.newBuilder()
                .setTaskQueue(workflowTaskQueue)
                .setWorkflowId(fullWorkflowId)
                .build();

        AnalysisWorkflow workflow = workflowClient.newWorkflowStub(AnalysisWorkflow.class, options);
        WorkflowClient.start(workflow::regenerate, jobId.toString(), record.getTranscriptObjectKey(), asrTaskQueue, record.getTargetBucket());

        record.setStatus("PROCESSING");
        record.setCurrentStep("REGENERATING ANALYSIS");
        repository.save(record);

        return new CreateJobResponse(jobId, fullWorkflowId, "STARTED", Instant.now());
    }

    public JobStatusResponse getWorkflowStatus(UUID jobId, String workflowId) {
        JobRecord record = repository.findById(jobId)
                .orElseThrow(() -> new IllegalArgumentException("Job not found: " + jobId));

        // Reconstruct the full ID if only a suffix was provided in the URL (e.g., "2" -> "asr-job-...-2")
        String effectiveWorkflowId = workflowId;
        if (!workflowId.startsWith("asr-job-") && !workflowId.startsWith("analysis-regen-")) {
            // Try to guess based on standard patterns or just try both.
            // For ASR retry, it's asr-job-{jobId}-{suffix}
            effectiveWorkflowId = "asr-job-" + jobId + "-" + workflowId;
        }

        try {
            WorkflowJobStatus live = null;
            if (effectiveWorkflowId.startsWith("asr-job-")) {
                AsrWorkflow workflow = workflowClient.newWorkflowStub(AsrWorkflow.class, effectiveWorkflowId);
                live = workflow.getStatus();
            } else if (effectiveWorkflowId.startsWith("analysis-regen-")) {
                AnalysisWorkflow workflow = workflowClient.newWorkflowStub(AnalysisWorkflow.class, effectiveWorkflowId);
                live = workflow.getStatus();
            }

            if (live != null) {
                record.setStatus(live.status());
                record.setCurrentStep(live.currentStep());
                record.setProgressPercent(live.progressPercent());
                record.setTotalChunks(live.totalChunks());
                record.setCompletedChunks(live.completedChunks());
                record.setTranscriptObjectKey(live.transcriptObjectKey());
                record.setErrorMessage(live.errorMessage());

                if (effectiveWorkflowId.startsWith("asr-job-")) {
                    repository.save(record);
                }

                return new JobStatusResponse(
                        record.getId(),
                        effectiveWorkflowId,
                        live.status(),
                        live.currentStep(),
                        Optional.ofNullable(live.progressPercent()).orElse(0),
                        Optional.ofNullable(live.totalChunks()).orElse(0),
                        Optional.ofNullable(live.completedChunks()).orElse(0),
                        record.getInputObjectKey(),
                        live.transcriptObjectKey() != null ? live.transcriptObjectKey() : record.getTranscriptObjectKey(),
                        live.errorMessage(),
                        record.getCreatedAt(),
                        record.getUpdatedAt());
            }
        } catch (RuntimeException ignored) {
            // If the reconstructed ID fails (e.g. it was actually a regen ID), try the other one
            if (!workflowId.startsWith("asr-job-") && !workflowId.startsWith("analysis-regen-") && effectiveWorkflowId.startsWith("asr-job-")) {
                String secondAttemptId = "analysis-regen-" + jobId + "-" + workflowId;
                try {
                    AnalysisWorkflow workflow = workflowClient.newWorkflowStub(AnalysisWorkflow.class, secondAttemptId);
                    WorkflowJobStatus live = workflow.getStatus();
                    if (live != null) {
                        return new JobStatusResponse(
                                record.getId(),
                                secondAttemptId,
                                live.status(),
                                live.currentStep(),
                                Optional.ofNullable(live.progressPercent()).orElse(0),
                                Optional.ofNullable(live.totalChunks()).orElse(0),
                                Optional.ofNullable(live.completedChunks()).orElse(0),
                                record.getInputObjectKey(),
                                live.transcriptObjectKey() != null ? live.transcriptObjectKey() : record.getTranscriptObjectKey(),
                                live.errorMessage(),
                                record.getCreatedAt(),
                                record.getUpdatedAt());
                    }
                } catch (RuntimeException ignored2) {}
            }
        }

        return map(record);
    }

    private JobStatusResponse map(JobRecord record) {
        return new JobStatusResponse(
                record.getId(),
                record.getWorkflowId(),
                record.getStatus(),
                record.getCurrentStep(),
                Optional.ofNullable(record.getProgressPercent()).orElse(0),
                Optional.ofNullable(record.getTotalChunks()).orElse(0),
                Optional.ofNullable(record.getCompletedChunks()).orElse(0),
                record.getInputObjectKey(),
                record.getTranscriptObjectKey(),
                record.getErrorMessage(),
                record.getCreatedAt(),
                record.getUpdatedAt());
    }

    @WorkflowInterface
    public interface AsrWorkflow {
        @WorkflowMethod
        void start(StartAsrRequest request);

        @QueryMethod
        WorkflowJobStatus getStatus();
    }

    @WorkflowInterface
    public interface AnalysisWorkflow {
        @WorkflowMethod
        void regenerate(String jobId, String transcriptKey, String asrTaskQueue, String bucket);

        @QueryMethod
        WorkflowJobStatus getStatus();
    }

    public record StartAsrRequest(String jobId, String inputObjectKey, String mediaTaskQueue, String asrTaskQueue, String targetBucket) {}
    public record WorkflowJobStatus(
            String status,
            String currentStep,
            Integer progressPercent,
            Integer totalChunks,
            Integer completedChunks,
            String transcriptObjectKey,
            String errorMessage) {}
}
