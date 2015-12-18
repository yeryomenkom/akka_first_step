import akka.actor.UntypedActor;

import java.io.EOFException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by Misha on 12/18/2015.
 */
public class Worker extends UntypedActor {
    private static final Pattern patternMain = Pattern.compile("\"[0-9]+;[0-9]+\"");
    private static final Pattern patternDigits = Pattern.compile("[0-9]+");

    @Override
    public void onReceive(Object message) throws Exception {
        if (message instanceof TaskMessage) {
            HashMap<Integer, Long> sumById = parseFileAndGetResult((TaskMessage) message);
            getSender().tell(new ResultMessage(sumById), getSelf());
        } else unhandled(message);
    }

    private HashMap<Integer, Long> parseFileAndGetResult(TaskMessage msg) throws IOException {
        StringBuilder sb = new StringBuilder();
        RandomAccessFile file = new RandomAccessFile(msg.targetFilePath, "r");

        file.seek(msg.beginIndex);
        for (long i=msg.beginIndex; i<msg.endIndex; i++) {
            sb.append((char) file.read());
        }

        try {
            while (sb.charAt(sb.length()-2) == '"' || sb.charAt(sb.length()-1) == ';') {
                sb.append(file.readChar());
            }
        } catch (EOFException e) {

        }

        file.close();

        HashMap<Integer, Long> sumById = new HashMap<>();

        Matcher matcher = patternMain.matcher(sb.toString());
        Matcher matcherDigits;
        while (matcher.find()) {
            matcherDigits = patternDigits.matcher(matcher.group());
            matcherDigits.find();
            int id = Integer.parseInt(matcherDigits.group());
            matcherDigits.find();
            int volume = Integer.parseInt(matcherDigits.group());
            sumById.put(id, sumById.getOrDefault(id, (long) 0) + volume);
        }

        return sumById;
    }

    static class TaskMessage {
        public final long beginIndex, endIndex;
        public final String targetFilePath;

        public TaskMessage(long beginIndex, long endIndex, String targetFilePath) {
            this.beginIndex = beginIndex;
            this.endIndex = endIndex;
            this.targetFilePath = targetFilePath;
        }
    }

    static class ResultMessage {
        public final Map<Integer, Long> sumById;

        public ResultMessage(Map<Integer, Long> sumById) {
            this.sumById = sumById;
        }
    }
}
