package com.wells.flink.demo.table;

import com.wells.flink.demo.entity.Person;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;

import java.util.LinkedList;
import java.util.List;

/**
 * Description Flink SQL Test
 * Created by wells on 2020-05-27 09:45:13
 */

public class SQLApiTest {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tEnv = BatchTableEnvironment.create(env);

        List<Person> data = new LinkedList<Person>();
        data.add(new Person("Tom", 12, "女", 1590306357000L));
        data.add(new Person("Jerry", 13, "男", 1590306358000L));
        data.add(new Person("Tom", 14, "女", 1590306359000L));
        data.add(new Person("Tom", 25, "男", 1590306360000L));
        data.add(new Person("Tom", 6, "女", 1590306362000L));
        data.add(new Person("Tom", 82, "女", 1590306367000L));
        data.add(new Person("Tom", 31, "男", 1590306377000L));
        data.add(new Person("Jerry", 31, "男", 1590306387000L));
        data.add(new Person("Jerry", 49, "男", 1590306397000L));
        data.add(new Person("Jerry", 108, "女", 1590306457000L));

        DataSource<Person> personDataSource = env.fromCollection(data);

        tEnv.createTemporaryView("personTable", personDataSource);
        Table resTable = tEnv.sqlQuery("select * from personTable");

        DataSet<Person> personDataSet = tEnv.toDataSet(resTable, Person.class);
        personDataSet.print();
    }
}
