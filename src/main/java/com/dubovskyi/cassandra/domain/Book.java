package com.dubovskyi.cassandra.domain;

import lombok.Data;

import java.util.UUID;

@Data
public class Book {
    private UUID id;

    private String title;

    private String author;

    private String subject;

    private String publisher;
    private String date;

}
