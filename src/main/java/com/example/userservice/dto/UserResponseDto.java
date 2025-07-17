package com.example.userservice.dto;

import lombok.Data;

@Data
public class UserResponseDto {
    private Long id;
    private String name;
    private String email;
    private String phone;
    private Integer totalOrders;
    private Double totalSpent;
}
