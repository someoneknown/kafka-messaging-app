import org.apache.commons.io.input.ReversedLinesFileReader;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class User {
    private final String id;
    private Set<String> subscribedGroups;
    private File groupFile;
    private File msgFile;
    private ConsumerService consumerService;

    public User(String id) throws IOException {
        this.id = id;
        subscribedGroups = new HashSet<>();
        groupFile = new File("src/main/resources/" + id + "-group.txt");
        groupFile.createNewFile();
        msgFile = new File("src/main/resources/" + id + "-msg.txt");
        msgFile.createNewFile();
        Scanner sc = new Scanner(groupFile);
        while(sc.hasNextLine()) {
            subscribedGroups.add(sc.nextLine());
        }
        sc.close();
        consumerService = new ConsumerService(id);
    }

    private void addSubscribedGroups(String groupName) {
        consumerService.pendingTopics.push(groupName);
    }

    private void startConsumer() {
        List<String> topicsToReadFrom = new ArrayList<>(subscribedGroups);
        topicsToReadFrom.replaceAll(s -> s + "-grp");
        topicsToReadFrom.add(id + "-user");
        Thread consumerThread = new Thread(() -> {
            try {
                consumerService.readMessage(topicsToReadFrom, msgFile, (groupName) -> {
                    //File writing
                    try (BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(groupFile, true))) {
                        bufferedWriter.write(groupName + "\n");
                        bufferedWriter.flush();
                        subscribedGroups.add(groupName);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        consumerThread.start();
    }

    private void read() throws IOException {
        ReversedLinesFileReader rlfr = new ReversedLinesFileReader(msgFile, StandardCharsets.UTF_8);
        Stack<String> latestMessages = new Stack<>();
        for(int i = 0; i < 5; ++i) {
            String message = rlfr.readLine();
            if(message == null) {
                break;
            }
            latestMessages.push(message);
        }
        while(!latestMessages.empty()) {
            System.out.println(latestMessages.pop());
        }
    }

    public void sendMsgToGroup(String groupName, String msg) {
        if(!subscribedGroups.contains(groupName)) {
            addSubscribedGroups(groupName);
        }
        ProducerService.sendMessage(groupName + "-grp", new Message(id, null, groupName, msg));
    }

    public void sendMsgToUser(String receiverId, String msg) {
        System.out.println("Msg " + msg);
        ProducerService.sendMessage(receiverId + "-user", new Message(id, receiverId, null, msg));
    }
    private void sendMenu() throws IOException {
        Scanner sc = new Scanner(System.in);
        while(true) {
            System.out.println("0.To User\n1.To group\n2.Back");
            int choice = sc.nextInt();
            sc.nextLine();
            switch (choice) {
                case 0:
                    System.out.println("Enter user ID");
                    String userId = sc.nextLine();
                    System.out.println("Enter message");
                    String msg = sc.nextLine();
                    sendMsgToUser(userId, msg);
                    break;
                case 1:
                    System.out.println("Enter group ID");
                    String groupId = sc.nextLine();
                    System.out.println("Enter message");
                    msg = sc.nextLine();
                    sendMsgToGroup(groupId, msg);
                    break;
                case 2:
                    mainMenu();
                    break;
                default:
                    System.out.println("Invalid Choice\n");
            }
        }
    }
    private void mainMenu() throws IOException {
        Scanner sc = new Scanner(System.in);
        while(true) {
            System.out.println("Enter your choice: \n0.Read\n1.Send\n2.Join Group");
            int choice = sc.nextInt();
            sc.nextLine();
            switch (choice) {
                case 0 :
                    read();
                    break;
                case 1:
                    sendMenu();
                    break;
                case 2:
                    addSubscribedGroups(sc.nextLine());
                    break;
                default:
                    System.out.println("Invalid Choice");
                    mainMenu();
            }
        }
    }

    public static void main(String[] args) throws IOException {
        Scanner sc = new Scanner(System.in);
        System.out.println("Enter User Id");
        String id = sc.next();
        User user = new User(id);
        user.startConsumer();
        user.mainMenu();
    }
}
