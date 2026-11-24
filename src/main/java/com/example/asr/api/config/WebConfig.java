package com.example.asr.api.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

@Configuration
public class WebConfig implements WebMvcConfigurer {
    @Override
    public void addCorsMappings(CorsRegistry registry) {
        registry.addMapping("/**")
                .allowedOrigins(
                        "http://localhost:5173",
                        "http://localhost:2400",
                        "https://dev.d-reams.com",
                        "http://localhost:3400",
                        "http://localhost:8083",
                        "https://gerbang.lokal.my/dreams/api/vtt/v1",
                        "http://localhost:9080"

                )
                .allowedMethods("GET", "POST", "DELETE", "OPTIONS");
    }
}
