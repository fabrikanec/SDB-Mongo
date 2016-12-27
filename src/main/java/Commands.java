import com.mongodb.MongoClient;
import redis.clients.jedis.Jedis;

import java.sql.Blob;
import java.sql.ResultSet;
import java.util.*;
import java.sql.Date;

interface Operation {
    int call(String... expr) throws Exception;
}

public class Commands {
    private static final String url = "localhost";
    private static final Integer port = 27017;
    private List<Operation> ops;
    private Jedis jedis = new Jedis(url);
    private EngineDBLibrary lib = new EngineDBLibrary(url, port);

    public enum Opcodes {
        ADD, UPDATE, DELETE, LIST, BUYGAME, SEEACQGAMES, UPDATEMONEY, FINDGAMES
    }

    Commands() {
        ops = new ArrayList<>();
        ops.add(this::add);
        ops.add(this::update);
        ops.add(this::delete);
        ops.add(this::list);
        ops.add(this::buygame);
        ops.add(this::seeAcquiredGames);
        ops.add(this::updatemoney);
        ops.add(this::findgames);
    }

    public List<Operation> getOps() { return this.ops; }

    public int add(String... params) throws Exception {
        int res = 0;
        System.err.println("add called");
        // type
        switch(params[0]) {
            case "users":
                try {
                    res = lib.createUser(params[1]);
                    jedis.set(String.valueOf(params[1].hashCode()),params[1] + "0");
                    break;
                } catch(Exception e) {
                    System.err.println("add error " + params.length + params.toString());
                    throw e;
                }
            case "games":
                try {
                    res = lib.createGame(params[1], Integer.valueOf(params[2]), params[3],
                            null, params[5], Integer.valueOf(params[6]),
                            Date.valueOf(params[7]), Integer.valueOf(params[8]));
                    
                    jedis.set(String.valueOf(params[1].hashCode()), params[1] + " " 
                            + params[2] + " " 
                            + params[3] + " " 
                            + params[5] + " " 
                            + params[6] + " " 
                            + params[7] + " " 
                            + params[8]);

                } catch (Exception e) {
                    throw e;
                }
                break;
            case "communities":
                try {
                    res = lib.createCommunity(params[1], params[2]);
                    jedis.set(String.valueOf(params[1].hashCode()), params[1] + " " + params[2]);
                } catch(Exception e) {
                        throw e;
                }
                break;
            case "Events":
                try {
                    res = lib.createEvent(params[1],Integer.valueOf(params[2]), params[3], Integer.valueOf(params[4]), 
                                          java.sql.Date.valueOf(params[5]), Integer.valueOf(params[6]));
                    
                    jedis.set(String.valueOf(params[1].hashCode()), params[1] +" "+ params[2]+" " + params[3] + " "+
                                             Integer.valueOf(params[4])+" "+ params[5]+" "+params[6]);
                } catch(Exception e) {
                    throw e;
                }
                break;
            case "Articles":
                try {
                    res = lib.createArticles(params[1], params[2], Integer.valueOf(params[3]));
                    jedis.set(String.valueOf(params[1].hashCode()), params[1] +" "+ params[2]+" " + params[3]);
                } catch(Exception e) {
                    throw e;
                }
                break;
            case "developers":
                try {
                    res = lib.createDevelopers(params[1], params[2]);
                    jedis.set(String.valueOf(params[1].hashCode()), params[1] +" "+ params[2]);
                } catch(Exception e) {
                    throw e;
                }
                break;
            case "users_com":
                try {
                    res = lib.addUserToComminuty(params[1], params[2]);
                    jedis.set(String.valueOf(params[1].hashCode()), params[1] +" "+ params[2]);
                } catch(Exception e) {
                    throw e;
                }
                break;
            default:
                System.err.println("Table ``" + params[0] + "'' is not found.");
        }

        return res;
    }
    public int update(String... params) throws Exception {
        jedis.del(params[1]);
        int res = 0;
        System.out.println("Called update.");
        try {
            switch (params[0]) {
                case "users":
                    res = lib.updateUser(params[1], params[2], Integer.valueOf(params[3]));
                    break;
                case "games":
                    res = lib.updateGame(params[1], params[2], Integer.valueOf(params[3]),
                                          params[4], null, Integer.valueOf(params[5]));
                    break;
                case "communities":
                    res = lib.updateCommunity(params[1], params[2], params[3]);
                    break;
                case "Events":
                    res = lib.updateEvent(params[1], params[2],params[3],
                            Integer.valueOf(params[4]), java.sql.Date.valueOf(params[5]));
                    break;
                case "Articles":
                    res = lib.updateArticles(params[1], params[2],params[3]);
                    break;
                case "developers":
                    res = lib.updateDevelopers(params[1],params[2], params[3]);
                    break;
            }
        } catch(Exception e) {
            throw e;
        }
        return res;
    }
    public int delete(String... params) throws Exception{
        jedis.del(params[1]);
        int res = 0;
        System.out.println("Called delete " + params[0]);
        try{
            switch(params[0]) {
                case "users":
                    res = lib.deleteUser(params[1]);
                    break;
                case "games":
                    res = lib.deleteGame(params[1]);
                    break;
                case "communities":
                    res = lib.deleteCommunity(params[1]);
                    break;
                case "Events":
                    res = lib.deleteEvent(params[1]);
                    break;
                case "Articles":
                    res = lib.deleteArticles(params[1]);
                    break;
                case "developers":
                    res = lib.deleteDevelopers(params[1]);
                    break;
                case "users_com":
                    try {
                        res = lib.deleteUserFromCommunity(params[1], params[2]);
                        jedis.set(String.valueOf(params[1].hashCode()), params[1] +" "+ params[2]);
                    } catch(Exception e) {
                        throw e;
                    }
            }
        } catch(Exception e){
            throw e;
        }
        return res;
    }
    public int list(String... params) throws Exception{
        System.out.println("Called list");
        switch(params[0]) {
            case "users":
                try {
                    lib.viewUsers(params[1]);
                } catch(Exception e) {
                    throw e;
                }
                break;
            case "games":
                try {
                        lib.viewGames(params[1]);
                } catch(Exception e) {
                    throw e;
                }
                break;
                case "communities":
                    try {
                            lib.viewCommunities(params[1]);
                    } catch(Exception e) {
                        throw e;
                    }
                    break;
                case "Events":
                    try {
                            lib.viewEvents(params[1]);
                    }
                     catch(Exception e) {
                        throw e;
                    }
                    break;
                case "Articles":
                    try {
                            lib.viewArticles(params[1]);
                    } catch(Exception e) {
                        throw e;
                    }
                    break;
            case "developers":
                try {
                        lib.viewDevelopers(params[1]);
                } catch(Exception e) {
                    throw e;
                }
                break;
            case "users_com":
                try {
                    lib.viewUsersComminities(params[1]);
                    jedis.set(String.valueOf(params[1].hashCode()), params[1]);
                } catch(Exception e) {
                    throw e;
                }
                break;
        }
        return 0;
    }

    public int buygame(String... params) throws Exception{
        int res = 0;
        try {
            System.out.println("Called buygame");
            lib.buyGame(params[0], params[1]);
        }
        catch(Exception e){
            throw e;
        }
        return res;
    }

    public int seeAcquiredGames(String... params) throws Exception{
        int res = 0;
        try {
            System.out.println("Called seeAcquiredGames");
            lib.seeAcquiredGames();
        }
        catch(Exception e){
            throw e;
        }
        return res;
    }

    public int updatemoney(String... params) throws Exception{
        int res = 0;
        try{
            System.out.println("Called updatemoney");
            lib.updateUserMoney(params[0], Integer.valueOf(params[1]));
        }
        catch(Exception e){
            throw e;
        }
        return res;
    }

    public int findgames(String... params) throws Exception {
        int res = 0;
        try{
            System.out.println("Called findgames");
            lib.findGamesByGenre(params[0]);
        }
        catch(Exception e){
            throw e;
        }
        return res;
    }
}