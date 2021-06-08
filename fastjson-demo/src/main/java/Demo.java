import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.PropertyNamingStrategy;
import com.alibaba.fastjson.serializer.SerializeConfig;
import javafx.application.Application;
import javafx.stage.Stage;
import org.junit.Assert;

public class Demo extends Application {

    public static void main(String[] args) {

        // 生产环境中，config要做singleton处理，要不然会存在性能问题
        User user = new User();
        user.setNameInfo("coder");
        user.setAgeInfo("28");
        user.setSexInfo("男孩");
        String json;
        SerializeConfig config = new SerializeConfig();
        //config.propertyNamingStrategy = PropertyNamingStrategy.SnakeCase;
        //String json = JSON.toJSONString(user, config);
        //Assert.assertEquals("{\"age_info\":\"28\",\"name_info\":\"coder\",\"sex_info\":\"男孩\"}", json);

        //config.propertyNamingStrategy = PropertyNamingStrategy.CamelCase;//为 low Camel Case
        //json = JSON.toJSONString(user, config);
        //Assert.assertEquals("{\"ageInfo\":\"28\",\"nameInfo\":\"coder\",\"sexInfo\":\"男孩\"}", json);

        //config.propertyNamingStrategy = PropertyNamingStrategy.PascalCase;//也叫 Upper camel case
        //json = JSON.toJSONString(user, config);
        //Assert.assertEquals("{\"AgeInfo\":\"28\",\"NameInfo\":\"coder\",\"SexInfo\":\"男孩\"}", json);

        config.propertyNamingStrategy = PropertyNamingStrategy.KebabCase;
        json = JSON.toJSONString(user, config);
        Assert.assertEquals("{\"age-info\":\"28\",\"name-info\":\"coder\",\"sex-info\":\"男孩\"}", json);
        launch(args);
    }

    @Override
    public void start(Stage primaryStage) {

    }
}
