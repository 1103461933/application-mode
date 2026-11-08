import java.util.ServiceLoader;
import org.apache.hadoop.fs.azurebfs.extensions.CustomTokenProviderAdaptee;

public class ServiceLoaderTest {
    public static void main(String[] args) {
        ServiceLoader.load(CustomTokenProviderAdaptee.class)
            .forEach(provider ->
                System.out.println("PROVIDER FOUND: " +
                    provider.getClass().getName()));
    }
}
