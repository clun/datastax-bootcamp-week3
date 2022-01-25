package com.datastax.workshop;

import static com.datastax.workshop.TestUtils.TABLE_TODOITEMS;
import static com.datastax.workshop.TestUtils.TODO_COMPLETED;
import static com.datastax.workshop.TestUtils.TODO_ITEM_ID;
import static com.datastax.workshop.TestUtils.TODO_TITLE;
import static com.datastax.workshop.TestUtils.TODO_USER_ID;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchStatementBuilder;
import com.datastax.oss.driver.api.core.cql.BatchType;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.uuid.Uuids;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;

/**
 * Transfer/Copy all Todoitems from any one user_id to a new user_id 
 * with completed being false. At the very least one Todoitem should be created.
 * 
 * @author Cedrick LUNVEN (@clunven)
 */
public class HomeworkWeek3 {
    
    private CqlSession cqlSession;
    
    private PreparedStatement psInsertTodo;
    
    private PreparedStatement psSelectTodo;
    
    private PreparedStatement psDeleteTodo;
    
    public HomeworkWeek3(CqlSession cqlSession) {
        if (cqlSession == null) { 
            throw new IllegalArgumentException("CqlSession cannot be null");
        }
        this.cqlSession = cqlSession;
        
        // Always prepare your statements
        preparedStatements();
    }
   
    public void createTodo(String userId, String title) {
        if (userId == null || "".equals(userId)) { 
            throw new IllegalArgumentException("Initial user id is invalid");
        }
        if (title   == null || "".equals(title))  {
            throw new IllegalArgumentException("title is invalid");
        }
        cqlSession.execute(psInsertTodo.bind(userId, Uuids.timeBased(), title, false));
    }
    
    public void transferTodos(String userIdFrom, String useridTo, boolean deleteOld) {

        // Validate your parameters
        if (userIdFrom == null || "".equals(userIdFrom)) { 
            throw new IllegalArgumentException("Initial user id is invalid");
        }
        if (useridTo   == null || "".equals(useridTo))  {
            throw new IllegalArgumentException("Target user id is invalid");
        }
        
        // Get all Tasks from user FROM (allow as user_id is PK)
        ResultSet rs = cqlSession.execute(psSelectTodo.bind(userIdFrom));
        if (rs.getAvailableWithoutFetching() == 0) {
            throw new IllegalArgumentException("Initial user id has no todos.");
        }
        
        // Build copy the task. We will use the batch trick as we copy in the same partition
        BatchStatementBuilder batchCopy = BatchStatement.builder(BatchType.LOGGED);
        
        // Loop on rows (we know we will not get millions, fitting a batch ~100 records)
        rs.all().stream().forEach(row -> {
            batchCopy.addStatement(psInsertTodo.bind(
                    useridTo,                  // userId the new owner 
                    Uuids.timeBased(),         // itemId would like to update the timestamp
                    row.getString(TODO_TITLE), // Copy the title
                    false));                   // Make all tasks false
        });
                
        // Perform copy
        cqlSession.execute(batchCopy.build());
        
        // Delete all in once to create a partition level tombstone
        if (deleteOld) {
            cqlSession.execute(psDeleteTodo.bind(userIdFrom));
        }
        
    }
    
    private void preparedStatements() {
        psInsertTodo = cqlSession.prepare(QueryBuilder
                .insertInto(TABLE_TODOITEMS)
                .value(TODO_USER_ID, QueryBuilder.bindMarker())
                .value(TODO_ITEM_ID, QueryBuilder.bindMarker())
                .value(TODO_TITLE, QueryBuilder.bindMarker())
                .value(TODO_COMPLETED, QueryBuilder.bindMarker())
                .build());
        psSelectTodo = cqlSession.prepare(QueryBuilder
                .selectFrom(TABLE_TODOITEMS).column(TODO_TITLE)
                .whereColumn(TODO_USER_ID).isEqualTo(QueryBuilder.bindMarker())
                .build());
        psDeleteTodo = cqlSession.prepare(QueryBuilder
                .deleteFrom(TABLE_TODOITEMS)
                .whereColumn(TODO_USER_ID).isEqualTo(QueryBuilder.bindMarker())
                .build());
    }

}
