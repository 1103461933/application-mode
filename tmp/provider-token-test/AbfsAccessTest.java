import java.net.URI;
import java.nio.charset.StandardCharsets;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSDataInputStream;

public class AbfsAccessTest {

    public static void main(String[] args) throws Exception {

        String endpoint = "flinkstorage911.dfs.core.windows.net";
        String container = "application-mode-checkpoints";

        Configuration conf = new Configuration();

        conf.set(
            "fs.azure.account.auth.type." + endpoint,
            "Custom"
        );

        conf.set(
            "fs.azure.account.oauth.provider.type." + endpoint,
            "com.xlen.azure.WorkloadIdentityTokenProvider"
        );

        URI uri = new URI(
            "abfs://" + container + "@" + endpoint + "/"
        );

        Path testFile = new Path("/workload-identity-test.txt");
        String expected = "Azure Workload Identity ABFS test";

        try (FileSystem fs = FileSystem.get(uri, conf)) {

            try (FSDataOutputStream out = fs.create(testFile, true)) {
                out.write(expected.getBytes(StandardCharsets.UTF_8));
            }

            System.out.println("ABFS_WRITE_OK");

            String actual;

            try (FSDataInputStream in = fs.open(testFile)) {
                byte[] data = new byte[expected.length()];
                in.readFully(data);
                actual = new String(data, StandardCharsets.UTF_8);
            }

            if (!expected.equals(actual)) {
                throw new RuntimeException("Read content does not match");
            }

            System.out.println("ABFS_READ_OK");

            if (!fs.delete(testFile, false)) {
                throw new RuntimeException("Could not delete test file");
            }

            System.out.println("ABFS_DELETE_OK");
        }
    }
}
