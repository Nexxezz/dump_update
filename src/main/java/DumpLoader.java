import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.eclipse.egit.github.core.Repository;
import org.eclipse.egit.github.core.client.GitHubClient;
import org.eclipse.egit.github.core.service.RepositoryService;
import org.eclipse.egit.github.core.service.UserService;

import javax.xml.crypto.Data;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.types.DataTypes.LongType;
import static org.apache.spark.sql.types.DataTypes.StringType;


public class DumpLoader {
    private static final String TOKEN = "676f3546c8bc9a99c8f5e379d5e";

    static final SparkSession SPARK_SESSION = SparkSession
            .builder()
            .appName("Java Spark dump update example")
            .config("spark.master", "local")
            .getOrCreate();

    private Dataset<Row> watchersCSV = SPARK_SESSION.createDataFrame(Collections.emptyList(), new StructType()
            .add("user_id", LongType, true)
            .add("repo_id", LongType, true)
            .add("created_at", StringType, true));

    private Dataset<Row> usersCSV = SPARK_SESSION.createDataFrame(Collections.emptyList(), new StructType()
            .add("user_id", LongType, true)
            .add("login", StringType, true)
            .add("created_at", StringType, true)
            .add("country_code", StringType, true)
            .add("city", StringType, true));

    private Dataset<Row> projectsCSV = SPARK_SESSION.createDataFrame(Collections.emptyList(), new StructType()
            .add("repo_id", LongType, true)
            .add("owner_id", LongType, true)
            .add("name", StringType, true)
            .add("lang", StringType, true)
            .add("created_at", StringType, true)
            .add("updated_at", StringType, true));

    public void saveWatchersAsCSV(String path) {

        SPARK_SESSION.sparkContext().setLogLevel("ERROR");

        File archive_directory = new File(path);
        if (archive_directory.exists()) {
            File[] jsonFiles = archive_directory.listFiles();
            for (File file : jsonFiles) {
                Dataset<Row> batch = SPARK_SESSION.read().json(path + file.getName());
                Dataset<Row> watchers = batch.filter(col("type")
                        .equalTo("WatchEvent"))
                        .select(col("actor.id")
                                .as("user_id"), col("repo.id")
                                .as("repo_id"), col("created_at"));
                watchersCSV = watchersCSV.union(watchers);
            }
            watchersCSV.write().option("header", "true").csv(path + "result");
        } else System.out.println("Incorrect directory path");
    }

    public static UserService getUserService() {
        GitHubClient client = new GitHubClient();
        client.setOAuth2Token(TOKEN);
        return new UserService();
    }

    public static RepositoryService getRepositoryService() {
        GitHubClient client = new GitHubClient();
        client.setOAuth2Token(TOKEN);
        return new RepositoryService();
    }


    public Dataset<Row> updateRepositoriesGH(String path) {
        List<Row> repositories = new ArrayList<>();
        List<Repository> userRepositories = new ArrayList<>();
        File archive_directory = new File(path);

        if (archive_directory.exists()) {
            File[] jsonFiles = archive_directory.listFiles();

            for (File file : jsonFiles) {
                Dataset<Row> batch = SPARK_SESSION.read().json(path + file.getName());
                Dataset<Row> userId = batch.select("actor.id");
                Dataset<Row> login = batch.select("actor.login");

                for (Row row : userId.collectAsList()) {
                    try {
                        userRepositories = getRepositoryService().getRepositories(row.mkString());
                        for (Repository repo : userRepositories) {
                            repositories.add(RowFactory.create(repo.getId(), repo.getOwner(), repo.getCreatedAt(), repo.getLanguage(), repo.getName()));
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }

        return SPARK_SESSION.createDataFrame(repositories, new StructType()
                .add("repo_id", LongType, true)
                .add("owner_id", LongType, true)
                .add("name", StringType, true)
                .add("lang", StringType, true)
                .add("created_at", StringType, true)
                .add("updated_at", StringType, true));

    }

    public Dataset<Row> updateUsersGH(String path) {
        List<Row> users = new ArrayList<>();
        File archive_directory = new File(path);

        if (archive_directory.exists()) {
            File[] jsonFiles = archive_directory.listFiles();

            for (File file : jsonFiles) {
                Dataset<Row> batch = SPARK_SESSION.read().json(path + file.getName());
                Dataset<Row> userId = batch.select("actor.id");
                Dataset<Row> login = batch.select("actor.login");

                for (Row row : userId.collectAsList()) {
                    try {
                        String createdAt = getUserService().getUser(row.mkString()).getCreatedAt().toString();
                        String location = getUserService().getUser(row.mkString()).getLocation();
                        users.add(RowFactory.create(createdAt, location));
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
        return SPARK_SESSION.createDataFrame(users, new StructType()
                .add("created_at", StringType, true)
                .add("city", StringType, true));
    }

    public static void main(String[] args) {
        DumpLoader loader = new DumpLoader();
        loader.saveWatchersAsCSV(args[0]);
        loader.updateUsersGH(args[0]);
        loader.updateRepositoriesGH(args[0]);
    }
}






