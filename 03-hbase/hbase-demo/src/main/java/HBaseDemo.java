import com.ss.util.HBaseUtils;
import javafx.application.Application;
import javafx.stage.Stage;

import java.util.Arrays;
import java.util.List;

public class HBaseDemo extends Application {
    private static final String TABLE_NAME = "class";
    private static final String TEACHER = "teacher";
    private static final String STUDENT = "student";

    public static void main(String[] args) {
        System.out.println("fewfwefw");
        // 新建表
        List<String> columnFamilies = Arrays.asList(TEACHER, STUDENT);
        boolean table = HBaseUtils.createTable(TABLE_NAME, columnFamilies);
        System.out.println("表创建结果:" + table);
        launch(args);
    }

    @Override
    public void start(Stage primaryStage) {

    }
}
