import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.routing.SmallestMailboxPool;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Paths;
import java.util.Random;
import java.util.Scanner;
import java.util.stream.IntStream;

/**
 * Created by Misha on 12/18/2015.
 */
public class Main {

    public static void main(String[] args) {
        System.out.println("Creating test file...");
        String pathToTestFile = Paths.get("").toAbsolutePath().toString() + "\\demo.txt";
        createTestFile(pathToTestFile);
        System.out.println("Test file has been created. Test file path: " + pathToTestFile);

        System.out.println("Loading system...");
        ActorSystem system = ActorSystem.create("ActorSystem");
        ActorRef mainWorker = system.actorOf(Props.create(MainWorker.class), "MainWorker");
        ActorRef workers = system.actorOf(new SmallestMailboxPool(Runtime.getRuntime().availableProcessors())
                .props(Props.create(Worker.class)), "workers");

        System.out.println("Enter file path for start parsing.");
        System.out.println("For closing app enter word \"close\"");
        Scanner scanner = new Scanner(System.in);
        String buf;
        while (true) {
            buf = scanner.nextLine();
            if(buf.equals("close")) {
                system.shutdown();
                return;
            } else {
                mainWorker.tell(new MainWorker.WorkTask(workers, buf), ActorRef.noSender());
            }
        }
    }

    public static void createTestFile(String pathToFile) {
        File file = new File(pathToFile);

        String content = "\"%d;%d\";";

        int contentCount = 100_000;
        int idMax = 1000;

        file.delete();
        try (FileOutputStream fos = new FileOutputStream(file)) {
            Random random = new Random();
            PrintWriter writer = new PrintWriter(fos);

            IntStream.range(0, contentCount)
                    .forEach((i) -> writer.write(String.format(content, random.nextInt(idMax) + 1, random.nextInt())));

            writer.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
