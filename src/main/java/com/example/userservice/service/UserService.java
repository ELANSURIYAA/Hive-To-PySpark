package com.example.userservice.service;

import com.example.userservice.dto.UserRequestDto;
import com.example.userservice.dto.UserResponseDto;
import com.example.userservice.entity.User;
import com.example.userservice.exception.UserNotFoundException;
import com.example.userservice.repository.UserRepository;
import com.example.userservice.talonone.TalonOneClient;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.BeanUtils;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@RequiredArgsConstructor
public class UserService {
    private final UserRepository userRepository;
    private final TalonOneClient talonOneClient;

    @Transactional
    public UserResponseDto registerUser(UserRequestDto dto) {
        if (userRepository.findByEmail(dto.getEmail()).isPresent()) {
            throw new IllegalArgumentException("Email already exists");
        }
        User user = User.builder()
                .name(dto.getName())
                .email(dto.getEmail())
                .phone(dto.getPhone())
                .totalOrders(dto.getTotalOrders() != null ? dto.getTotalOrders() : 0)
                .totalSpent(dto.getTotalSpent() != null ? dto.getTotalSpent() : 0.0)
                .build();
        user = userRepository.save(user);
        // Register user in Talon.One
        boolean talonSuccess = talonOneClient.registerUser(user.getId().toString(), user.getEmail(), user.getName());
        if (!talonSuccess) {
            throw new RuntimeException("Failed to register user in Talon.One");
        }
        return toResponseDto(user);
    }

    public UserResponseDto getUserById(Long id) {
        User user = userRepository.findById(id)
                .orElseThrow(() -> new UserNotFoundException("User not found with id: " + id));
        return toResponseDto(user);
    }

    public UserResponseDto getUserByEmail(String email) {
        User user = userRepository.findByEmail(email)
                .orElseThrow(() -> new UserNotFoundException("User not found with email: " + email));
        return toResponseDto(user);
    }

    @Transactional
    public UserResponseDto updateUser(Long id, UserRequestDto dto) {
        User user = userRepository.findById(id)
                .orElseThrow(() -> new UserNotFoundException("User not found with id: " + id));
        if (dto.getName() != null) user.setName(dto.getName());
        if (dto.getEmail() != null) user.setEmail(dto.getEmail());
        if (dto.getPhone() != null) user.setPhone(dto.getPhone());
        if (dto.getTotalOrders() != null) user.setTotalOrders(dto.getTotalOrders());
        if (dto.getTotalSpent() != null) user.setTotalSpent(dto.getTotalSpent());
        user = userRepository.save(user);
        return toResponseDto(user);
    }

    @Transactional
    public void deleteUser(Long id) {
        if (!userRepository.existsById(id)) {
            throw new UserNotFoundException("User not found with id: " + id);
        }
        userRepository.deleteById(id);
    }

    private UserResponseDto toResponseDto(User user) {
        UserResponseDto dto = new UserResponseDto();
        BeanUtils.copyProperties(user, dto);
        return dto;
    }
}
