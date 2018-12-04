package com.dubovskyi.cassandra;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.dubovskyi.cassandra.repository.KeyspaceRepository;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.stream.Collectors;

public class CassandraConnectorTest {
    private Session session;
    private KeyspaceRepository schemaRepository;

    @Before
    public void connect() {
        CassandraConnector client = new CassandraConnector();
        client.connect("127.0.0.1", 9042);


        this.session = client.getSession();
        schemaRepository = new KeyspaceRepository(session);
    }

    @Test
    public void whenCreatingAKeyspace_thenCreated() {
        String keyspaceName = "library";
        schemaRepository.createKeyspace(keyspaceName, "SimpleStrategy", 1);

        ResultSet result = session.execute("SELECT * FROM system_schema.keyspaces;");

        System.out.println("result: "+ result.all());

        List<String> matchedKeyspaces = result.all()
                .stream()
                .filter(r -> r.getString(0).equals(keyspaceName.toLowerCase()))
                .map(r -> r.getString(0))
                .collect(Collectors.toList());


       // assertEquals(matchedKeyspaces.size(), 1);
      //  assertTrue(matchedKeyspaces.get(2).equals(keyspaceName.toLowerCase()));
    }


}