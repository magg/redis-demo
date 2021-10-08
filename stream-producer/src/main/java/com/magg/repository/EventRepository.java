package com.magg.repository;

import com.magg.model.TransactionModel;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.springframework.stereotype.Repository;

@Repository
public class EventRepository
{

    public static final List<TransactionModel> TransactionModel_LIST = Stream.of(
    new TransactionModel(1, "foo"),
    new TransactionModel(2, "foo"),
    new TransactionModel(3, "foo"),
    new TransactionModel(4, "foo"),
    new TransactionModel(5, "foo"),
    new TransactionModel(6, "bar"),
    new TransactionModel(7, "bar"),
    new TransactionModel(8, "bar"),
    new TransactionModel(9, "foo"),
    new TransactionModel(10, "foo"),
    new TransactionModel(11, "foo"),
    new TransactionModel(12, "foo"),
    new TransactionModel(13, "bar"),
    new TransactionModel(14, "bar"),
    new TransactionModel(15, "foo"),
    new TransactionModel(16, "bar"),
    new TransactionModel(17, "bar"),
    new TransactionModel(18, "bar"),
    new TransactionModel(19, "bar"),
    new TransactionModel(20, "foo"),
    new TransactionModel(21, "foo"),
    new TransactionModel(22, "foo"),
    new TransactionModel(23, "foo"),
    new TransactionModel(24, "bar"),
    new TransactionModel(25, "foo")
).collect(Collectors.toList());

    public TransactionModel getRandomTransactionModel() {
        int index = ThreadLocalRandom.current().nextInt(0, 25);
        return TransactionModel_LIST.get(index);
    }
}
