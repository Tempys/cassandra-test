package com.dubovskyi.cassandra;


import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.dubovskyi.cassandra.domain.Book;
import com.dubovskyi.cassandra.repository.BookRepository;
import com.dubovskyi.cassandra.repository.KeyspaceRepository;
import org.junit.Before;
import org.junit.Test;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;

public class BookRepositoryIntegrationTest {
    private KeyspaceRepository schemaRepository;

    private BookRepository bookRepository;

    private Session session;

    final String KEYSPACE_NAME = "library";
    final String BOOKS = "books";
    final String BOOKS_BY_TITLE = "booksByTitle";

    @Before
    public void connect() {
        CassandraConnector client = new CassandraConnector();
        client.connect("127.0.0.1", 9042);
        this.session = client.getSession();
        schemaRepository = new KeyspaceRepository(session);
        schemaRepository.createKeyspace(KEYSPACE_NAME, "SimpleStrategy", 1);
        schemaRepository.useKeyspace(KEYSPACE_NAME);
        bookRepository = new BookRepository(session);
    }


    @Test
    public void createTable(){
        bookRepository.createTable();
    }

    @Test
    public void simpleInsert(){


        Book book = new Book();
        book.setAuthor("anderson");
        book.setId(UUID.randomUUID());
        book.setSubject("cassandra");
        book.setTitle("indtroduction");
        book.setPublisher("publisher");
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
        book.setDate(df.format(new Date()));

        bookRepository.insertbook(book,"books");

    }

    @Test
    public void insert_1_mln(){

        long start = System.currentTimeMillis();
        for(int i =0;i< 100_000 ;i++){

            Book book = new Book();
            book.setAuthor("anderson"+i);
            book.setId(UUID.randomUUID());
            book.setSubject("cassandra"+i);
            book.setTitle("indtroduction"+i);
            book.setPublisher("publisher"+i);
            DateFormat df = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
            book.setDate(df.format(new Date()));

            bookRepository.insertbook(book,"books_index1");
          //  if(i%500 == 1 )sleep();
        }

        System.out.println("insert 1 mln rows for :"+(System.currentTimeMillis() - start));
    }


    public void sleep(){
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void selectCountLines(){
        long item = bookRepository.getCountOfItem();
        System.out.println(item);

    }

    @Test
    public void selectSimpleTimeRange(){
        long start = System.currentTimeMillis();
        ResultSet rows = bookRepository.selectTimePerid();
        System.out.println("select 100k rows for :"+(System.currentTimeMillis() - start));
        System.out.println( rows.one());

    }

    @Test
    public void selectTimeRangeWithIndex(){
        long start = System.currentTimeMillis();
        ResultSet rows = bookRepository.selectTimeWithIndex();
        System.out.println("select 100k rows for :"+(System.currentTimeMillis() - start));
        System.out.println( rows.one());
    }

}
