package com.example.userservice.dto;

import lombok.Data;

@Data
public class UserRequestDto {
    private String name;
    private String email;
    private String phone;
    private Integer totalOrders;
    private Double totalSpent;
}
