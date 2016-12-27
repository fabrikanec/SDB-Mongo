import java.io.*;
import java.util.*;

import com.mongodb.DB;
import com.mongodb.MongoClient;
import redis.clients.jedis.*;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisException;

public class Main {

    public static Commands.Opcodes getCmd(String cmd) {
        Commands.Opcodes cmd_type;
        switch (cmd) {
            case "add":
                cmd_type = Commands.Opcodes.ADD;
                break;
            case "update":
                cmd_type = Commands.Opcodes.UPDATE;
                break;
            case "delete":
                cmd_type = Commands.Opcodes.DELETE;
                break;
            case "list":
                cmd_type = Commands.Opcodes.LIST;
                break;
            case "buymusic":
                cmd_type = Commands.Opcodes.BUYmusic;
                break;
            case "seeAcqmusics":
                cmd_type = Commands.Opcodes.SEEACQmusicS;
                break;
            case "updatemoney":
                cmd_type = Commands.Opcodes.UPDATEMONEY;
                break;
            case "findmusics":
                cmd_type = Commands.Opcodes.FINDmusicS;
                break;
            default:
                System.err.println("Error: ``" + cmd + "`` not found.");
                return null;
        }
        return cmd_type;
    }

    public void run() {
        String[] tokens;
        String line;
        Scanner sc = new Scanner(System.in);
        Commands cmd = new Commands();
        Commands.Opcodes cmd_type;
        while (sc.hasNextLine()) {
            try {
                line = sc.nextLine();
                tokens = line.split(" ");
                cmd_type = getCmd(tokens[0]);

                String[] params = new String[tokens.length - 1];
                System.arraycopy(tokens, 1, params, 0, tokens.length - 1);
                if (cmd.getOps().get(cmd_type.ordinal()).call(params) != 0) {
                    System.err.println("Wrong arguments.");
                } else {
                    System.out.println("OK");
                }
            } catch (NullPointerException e) {
                continue;
            } catch(com.mongodb.DuplicateKeyException e){
                System.out.println("Duplicate.");
                continue;
            } catch (Exception e) {
                e.printStackTrace();
                return;
            }
        }
    }

    public static void main(String... args) {
        new Main().run();
    }
}