package com.datastax.workshop;

import static com.datastax.oss.driver.api.core.type.DataTypes.BOOLEAN;
import static com.datastax.oss.driver.api.core.type.DataTypes.TEXT;
import static com.datastax.oss.driver.api.core.type.DataTypes.TIMEUUID;
import static com.datastax.workshop.TestUtils.TABLE_TODOITEMS;
import static com.datastax.workshop.TestUtils.TODO_COMPLETED;
import static com.datastax.workshop.TestUtils.TODO_ITEM_ID;
import static com.datastax.workshop.TestUtils.TODO_TITLE;
import static com.datastax.workshop.TestUtils.TODO_USER_ID;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;

public class HomeworkWeek3Tests {
    
    
    @Test
    public void test() {
        
        // -- INIT DB --
        
        // We consider that the keyspace exist
        CqlSession cqlSession = TestUtils.createCqlSession();
        
        // Create table if needed
        cqlSession.execute(SchemaBuilder.createTable(TABLE_TODOITEMS).ifNotExists()
                .withPartitionKey(TODO_USER_ID, TEXT)
                .withClusteringColumn(TODO_ITEM_ID, TIMEUUID)
                .withColumn(TODO_COMPLETED, BOOLEAN)
                .withColumn(TODO_TITLE, TEXT)
                .withClusteringOrder(TODO_ITEM_ID, ClusteringOrder.ASC)
                .build());
        
        // Empty Table if needed (reproducable tests)
        cqlSession.execute(QueryBuilder.truncate(TABLE_TODOITEMS).build());
        
        
        // -- INIT --
        
        HomeworkWeek3 homework = new HomeworkWeek3(cqlSession);
        
        
        // Invalid userid
        Assertions.assertThrows(IllegalArgumentException.class, () -> homework.transferTodos("", "cedrick", false));
        // Invalid userid
        Assertions.assertThrows(IllegalArgumentException.class, () -> homework.transferTodos("john", null, false));
        // John has no Task
        Assertions.assertThrows(IllegalArgumentException.class, () -> homework.transferTodos("john", "cedrick", false));
        
        // When
        homework.createTodo("john", "Task1");
        homework.createTodo("john", "Task2");
        homework.createTodo("john", "Task3");
        
        // Then
        homework.transferTodos("john", "cedrick", false);
    }
    
    

}
