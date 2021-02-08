import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.io.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class Message implements Serializable {
    private static final long serialVersionUID = 6529685098267757690L;

    private String senderId;
    public String receiverId;
    private String groupId;
    private String content;
    private LocalDateTime timestamp;

    public Message() {

    }
    public Message(String senderId, String receiverId, String groupId, String content) {
        this.senderId = senderId;
        this.receiverId = receiverId;
        this.groupId = groupId;
        this.content = content;
        this.timestamp = LocalDateTime.now();
    }

    public String getMessage() {
        String message = "";
        if(groupId != null) {
            message = "[" + groupId + "]";
        }
        message += "[" + senderId + "]" + "[" + DateTimeFormatter.ofPattern("H:m:s d/M/Y").format(timestamp) + "] " + content;
        return message;
    }


    public static class MessageSerializer implements Serializer<Message> {

        @Override
        public byte[] serialize(String s, Message msg) {

            ByteArrayOutputStream b = new ByteArrayOutputStream();
            ObjectOutputStream o;
            try {
                o = new ObjectOutputStream(b);
                o.writeObject(msg);
            } catch (IOException e) {
                e.printStackTrace();
            }
            return b.toByteArray();
        }
    }

    public static class MessageDeserializer implements Deserializer<Message> {

        @Override
        public Message deserialize(String s, byte[] bytes) {
            ByteArrayInputStream byteStream = new ByteArrayInputStream(bytes);
            ObjectInputStream objStream;
            Message resultMsg = null;
            try {
                objStream = new ObjectInputStream(byteStream);
                resultMsg = (Message) objStream.readObject();
            } catch (IOException | ClassNotFoundException e) {
                e.printStackTrace();
            }
            return resultMsg;
        }
    }
}
