import java.io.*;
import java.util.*;

import com.mongodb.DB;
import com.mongodb.MongoClient;
import redis.clients.jedis.*;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisException;

public class Main {

    public void run( ) {

        String[] tokens = null;
        String   line   = null;

        Commands cmd = new Commands();
        Scanner sc = new Scanner(System.in);
        Commands.Opcodes cmd_type;

        while( true ) {
            try{
                if(sc.hasNextLine())
                    if((line = sc.nextLine() ) == null)
                        continue;
            } catch( IOError e ) {
                System.err.println("I/O error.");
                continue;
            }
            // cmd params
            tokens = line.split(" ");
            /*if( tokens == null ) {
                System.err.println("Bad input: ``" + line + "''." );
            }*/

            // cmd
            switch( tokens[0] ) {
                case "add":       cmd_type = Commands.Opcodes.ADD;       break;
                case "update":    cmd_type = Commands.Opcodes.UPDATE;    break;
                case "delete":    cmd_type = Commands.Opcodes.DELETE;    break;
                case "list":      cmd_type = Commands.Opcodes.LIST;      break;
                case "buygame":   cmd_type = Commands.Opcodes.BUYGAME;   break;
                case "seeAcqGames":   cmd_type = Commands.Opcodes.seeAcqGames;   break;
                case "updatemoney":   cmd_type = Commands.Opcodes.UPDATEMONEY;   break;
                case "findgames": cmd_type = Commands.Opcodes.FINDGAMES; break;
                default:
                    System.err.println("Error: ``" + tokens[0] + "'' is not found.");
                    continue;
            }
            String[] params = new String[tokens.length-1];
            System.arraycopy(tokens,1,params,0,tokens.length-1);
            try {
                if( cmd.getOps().get(cmd_type.ordinal()).call(params) != 0 )
                    System.err.println("Chto-to ne tak.");
                else
                    System.out.println("Vce norm");
            } catch ( Exception e ) {
                System.err.println( "Error: " + e);
            }
        }
    }

    public static void main (String... args) {
        new Main().run();
    }
}