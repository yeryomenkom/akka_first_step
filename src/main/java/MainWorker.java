import akka.actor.ActorRef;
import akka.actor.UntypedActor;

import java.io.*;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Misha on 12/18/2015.
 */
public class MainWorker extends UntypedActor {

    private static final int partsPerc = 5;

    private HashMap<Integer, Long> sumById = new HashMap<>();
    private String workFilePath;
    private int count;
    private boolean busy;

    @Override
    public void onReceive(Object message) throws Exception {
        if(message instanceof WorkTask) {
            WorkTask workTask = (WorkTask) message;
            if(busy) {
                System.out.println("Now system is busy. Try again later.");
                unhandled(message);
                return;
            }
            fillStateForNewTask(workTask);
            doWork(workTask.workers);
        } else if(message instanceof Worker.ResultMessage) {
            Map<Integer, Long> sumById = ((Worker.ResultMessage) message).sumById;
            sumById.forEach((k,v) -> this.sumById.put(k, this.sumById.getOrDefault(k,(long) 0) + v));
            if(--count == 0) {
                writeResultToFile();
                clearState();
            }
        } else unhandled(message);
    }

    private void fillStateForNewTask(WorkTask task) {
        busy = true;
        count = 100/partsPerc;
        workFilePath = task.pathToFile;
    }

    private void clearState() {
        busy = false;
        count = 0;
        sumById.clear();
        workFilePath = null;
    }

    private void doWork(ActorRef workers) throws IOException {
        try {
            RandomAccessFile workFile = new RandomAccessFile(workFilePath, "r");
            System.out.println("Work for file "+workFilePath+" starting...");

            long fileLength = workFile.length();
            long partSize = fileLength/count;
            for (long i=0, j ; i<workFile.length(); i=j) {
                j = i + partSize;
                workers.tell(new Worker.TaskMessage(i, j > fileLength ? fileLength : j, workFilePath), getSelf());
            }
            workFile.close();
        } catch (FileNotFoundException e) {
            System.out.println("File with path: " + workFilePath + " does not exist!\n");
        }
    }

    private void writeResultToFile() {
        File resultFile = new File(Paths.get("").toAbsolutePath().toString() + "\\result"+
                System.currentTimeMillis()+".txt");
        String content = "\"%d;%d\";";
        try (FileOutputStream fos = new FileOutputStream(resultFile)) {
            PrintWriter writer = new PrintWriter(fos);
            sumById.forEach((k,v) -> writer.write(String.format(content, k, v) + "\n"));
            writer.flush();
            System.out.println("Result for input file: " + workFilePath + " has been written to file: " + resultFile.getAbsolutePath());
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("Error while saving result for input file: " + workFilePath);
        }

    }

    static class WorkTask {
        public final ActorRef workers;
        public final String pathToFile;

        public WorkTask(ActorRef workers, String pathToFile) {
            this.workers = workers;
            this.pathToFile = pathToFile;
        }
    }
}
