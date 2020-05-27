package com.wells.flink.demo.table;

import com.wells.flink.demo.entity.Person;
import com.wells.flink.demo.entity.Word;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;

/**
 * Description Table API
 * Created by wells on 2020-05-26 06:07:55
 */

public class TableAPITest {
    public static void main(String[] args) throws Exception {
        wordCount();
        pojoCount();
    }

    public static void wordCount() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.createCollectionsEnvironment();
        BatchTableEnvironment tEnv = BatchTableEnvironment.create(env);

        DataSet<Word> input = env.fromElements(
                new Word("Java", 1L),
                new Word("Python", 2L),
                new Word("Scala", 1L));

        Table table = tEnv.fromDataSet(input);

        Table filtered = table
                .groupBy("word")
                .select("word, count.sum as count")
                .filter("count = 2");

        DataSet<Word> result = tEnv.toDataSet(filtered, Word.class);

        result.print();
    }

    public static void pojoCount() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tEnv = BatchTableEnvironment.create(env);

        DataSource<Person> personDataSource = env.fromElements(
                new Person("Tom", 12, "男", 1590306377000L),
                new Person("Jerry", 13, "女", 1590306457000L),
                new Person("Tom", 82, "女", 1590306367000L));

        Table table = tEnv.fromDataSet(personDataSource);
        Table resTable = table.groupBy("name").select("name, age.sum as age, sex, timestamp").filter("age=12");

        DataSet<Person> personDataSet = tEnv.toDataSet(resTable, Person.class);
        personDataSet.print();
    }
}
