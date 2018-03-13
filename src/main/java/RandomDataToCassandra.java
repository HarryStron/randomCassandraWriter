import com.datastax.driver.core.Session;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.apache.log4j.BasicConfigurator;

public class RandomDataToCassandra {

    public static void main(String[] args) throws IOException {
        BasicConfigurator.configure();

        ConnectionManager connectionManager = new ConnectionManager();
        Session session = connectionManager.connect("node", "username", "password", "cluster");

        List<Column> columns = Arrays.asList(
                new Column("example1", "[0-9]{10}"),
                new Column("example2", "[0-9]{10}")
        );

        new DatabaseWriter().populateWithData(session, "keyspaceName", "tableName", columns, 100);
        connectionManager.close();
    }
}