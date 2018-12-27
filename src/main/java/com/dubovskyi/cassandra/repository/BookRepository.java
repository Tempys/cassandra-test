package com.dubovskyi.cassandra.repository;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.dubovskyi.cassandra.domain.Book;

public class BookRepository {

    private static final String TABLE_NAME = "books";
    private static final String TABLE_NAME_BY_TITLE = TABLE_NAME + "ByTitle";
    private Session session;

    public BookRepository(Session session) {
        this.session = session;
    }

    /**
     * Creates the books table.
     */
    public void createTable() {
        StringBuilder sb = new StringBuilder("CREATE TABLE IF NOT EXISTS ").append(TABLE_NAME)
                                                                           .append("(").append("id uuid PRIMARY KEY, ")
                                                                                       .append("title text,")
                                                                                       .append("author text,")
                                                                                       .append("date timestamp,")
                                                                                       .append("subject text);");

        final String query = sb.toString();
        session.execute(query);
    }

    /**
     * Creates the books table.
     */
    public void createTableBooksByTitle() {
        StringBuilder sb = new StringBuilder("CREATE TABLE IF NOT EXISTS ").append(TABLE_NAME_BY_TITLE).append("(").append("id uuid, ").append("title text,").append("PRIMARY KEY (title, id));");

        final String query = sb.toString();
        session.execute(query);
    }

    /**
     * Alters the table books and adds an extra column.
     */
    public void alterTablebooks(String columnName, String columnType) {
        StringBuilder sb = new StringBuilder("ALTER TABLE ").append(TABLE_NAME).append(" ADD ").append(columnName).append(" ").append(columnType).append(";");

        final String query = sb.toString();
        session.execute(query);
    }

    /**
     * Insert a row in the table books.
     *
     * @param book
     */
    public void insertbook(Book book,String table) {
        StringBuilder sb = new StringBuilder("INSERT INTO ")
                                           .append(table).append("(id, title, author, subject,date) ")
                                           .append("VALUES (").append(book.getId()).append(", '").append(book.getTitle())
                                           .append("', '").append(book.getAuthor()).append("', '")
                                           .append(book.getSubject()).append("', '")
                                           .append(book.getDate())
                                           .append("');");

        final String query = sb.toString();
        session.execute(query);
    }

    /**
     * Insert a row in the table booksByTitle.
     * @param book
     */
    public void insertbookByTitle(Book book) {
        StringBuilder sb = new StringBuilder("INSERT INTO ").append(TABLE_NAME_BY_TITLE).append("(id, title) ").append("VALUES (").append(book.getId()).append(", '").append(book.getTitle()).append("');");

        final String query = sb.toString();
        session.execute(query);
    }

    /**
     * Insert a book into two identical tables using a batch query.
     *
     * @param book
     */
    public void insertBookBatch(Book book) {
        StringBuilder sb = new StringBuilder("BEGIN BATCH ").append("INSERT INTO ").append(TABLE_NAME).append("(id, title, author, subject) ").append("VALUES (").append(book.getId()).append(", '").append(book.getTitle()).append("', '").append(book.getAuthor())
                .append("', '").append(book.getSubject()).append("');").append("INSERT INTO ").append(TABLE_NAME_BY_TITLE).append("(id, title) ").append("VALUES (").append(book.getId()).append(", '").append(book.getTitle()).append("');")
                .append("APPLY BATCH;");

        final String query = sb.toString();
        session.execute(query);
    }

    public ResultSet selectTimePerid(){
       String request = "SELECT * FROM books  LIMIT 100 ALLOW FILTERING ";
       ResultSet rows = session.execute(request);
        System.out.println(rows.all().size());
        return  rows;
    }

    public ResultSet selectTimeWithIndex(){
        String request = "SELECT * FROM books_index1   LIMIT 100 ";
        ResultSet rows = session.execute(request);
        System.out.println(rows.all().size());
        return  rows;
    }

    public long getCountOfItem() {
        final String query = "select count(id) from books_small";
        ResultSet rs = session.execute(query);

        return rs.one().getLong(0);

    }

    /**
     * Delete a book by title.
     */
    public void deletebookByTitle(String title) {
        StringBuilder sb = new StringBuilder("DELETE FROM ").append(TABLE_NAME_BY_TITLE).append(" WHERE title = '").append(title).append("';");

        final String query = sb.toString();
        session.execute(query);
    }

    /**
     * Delete table.
     *
     * @param tableName the name of the table to delete.
     */
    public void deleteTable(String tableName) {
        StringBuilder sb = new StringBuilder("DROP TABLE IF EXISTS ").append(tableName);

        final String query = sb.toString();
        session.execute(query);
    }
}
