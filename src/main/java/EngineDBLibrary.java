import com.mongodb.*;
import redis.clients.jedis.Jedis;

import java.sql.*;
import java.sql.Date;
import java.util.*;


/**
 * Created by skyD9 on 13.11.2016.
 */
public class EngineDBLibrary {

    EngineDBLibrary(String url, Integer port){
        this.url = url;
        this.port = port;
    }

    private static String url="";
    private static Integer port =0;

    MongoClient mongo = new MongoClient( "localhost" , 27017);

    Jedis jedis = new Jedis(url);

    public Integer createUser(String nickName) throws Exception{
        Integer res;
        System.err.println(nickName);
        try{
            if (nickName.length() > 20)
                res = 1;
            else {
                DB db = mongo.getDB("EngineDB");
                DBCollection table = db.getCollection("users");
                BasicDBObject document = new BasicDBObject();
                document.put("name", nickName);
                document.put("money", 0);
                table.insert(document);
                res = 0;
            }
        }
        catch (Exception e){
            throw e;
        }
        return res;
    }

    public Integer updateUser(String nickName,String upnickName,Integer userMoney ) throws Exception{
        Integer res;

        try{
            if ((nickName.length() > 20) || (userMoney < 0))
                res = 1;
            else {
                DB db = mongo.getDB("EngineDB");
                DBCollection table = db.getCollection("users");

                BasicDBObject query = new BasicDBObject();
                query.put("name", nickName);

                BasicDBObject newDocument = new BasicDBObject();
                newDocument.put("name", upnickName);

                BasicDBObject updateObj = new BasicDBObject();
                updateObj.put("$set", newDocument);

                table.update(query, updateObj);
                res = 0;
            }
        }
        catch (Exception e){
            throw e;
        }
        return res;
    }

    public Integer deleteUser(String nickName) throws Exception{
        Integer res;

        try{
                DB db = mongo.getDB("EngineDB");
                DBCollection table = db.getCollection("users");

                BasicDBObject searchQuery = new BasicDBObject();
                searchQuery.put("name", nickName);

                table.remove(searchQuery);
            res = 0;
        }
        catch (Exception e){
            throw e;
        }
        return res;
    }

    public void viewUsers(String name) throws Exception{
        try{
            DB db = mongo.getDB("EngineDB");
            DBCollection table = db.getCollection("users");

            BasicDBObject searchQuery = new BasicDBObject();
            searchQuery.put("name", name);

            DBCursor cursor = table.find(searchQuery);

            while (cursor.hasNext()) {
                System.out.println(cursor.next());
                jedis.set(String.valueOf(cursor.next().hashCode()),String.valueOf(cursor.next()));
            }
        }
        catch (Exception e){
            throw e;
        }
    }

    public Integer updateUserMoney(String nickName, Integer userMoney) throws Exception{
        Integer res;

        try{
            if ((nickName.length() > 20) || (userMoney < 0))
                res = 1;
            else {
                DB db = mongo.getDB("EngineDB");
                DBCollection table = db.getCollection("users");

                BasicDBObject query = new BasicDBObject();
                query.put("name", nickName);

                BasicDBObject newDocument = new BasicDBObject();
                newDocument.put("money", userMoney);

                BasicDBObject updateObj = new BasicDBObject();
                updateObj.put("$set", newDocument);

                table.update(query, updateObj);
                res = 0;
            }
        }
        catch (Exception e){
            throw e;
        }
        return res;
    }

    public Integer findGamesByGenre(String genre) throws Exception{
        Integer res;

        try {
            DB db = mongo.getDB("EngineDB");
            DBCollection table = db.getCollection("games");

            BasicDBObject searchQuery = new BasicDBObject();
            searchQuery.put("genre", genre);

            DBCursor cursor = table.find(searchQuery);

            while (cursor.hasNext()) {
                System.out.println(cursor.next());
                jedis.set(String.valueOf(cursor.next().hashCode()), String.valueOf(cursor.next()));
            }
            res = 0;
        }
        catch (Exception e){
            throw e;
        }
        return res;
    }

    public Integer buyGame(String user_name, String game_name) throws Exception{
        Integer res;

        try{
            DB db = mongo.getDB("EngineDB");
            DBCollection table1 = db.getCollection("users");
            DBCollection table2 = db.getCollection("games");
            DBCollection table3 = db.getCollection("acquiredGames");

            //get money of the user
            BasicDBObject searchQuery = new BasicDBObject();
            searchQuery.put("name", user_name);
            DBObject result = table1.findOne(searchQuery);
            Integer money = Integer.valueOf(result.get("money").toString());

            //get cost of the game
            BasicDBObject searchQuery2 = new BasicDBObject();
            searchQuery2.put("name", game_name);
            DBObject result2 = table2.findOne(searchQuery2);
            Integer cost = Integer.valueOf(result2.get("cost").toString());

            if (cost <= money)
            {
                // update user money
                BasicDBObject query = new BasicDBObject();
                query.put("name", user_name);

                BasicDBObject newDocument = new BasicDBObject();
                newDocument.put("money", money - cost);

                BasicDBObject updateObj = new BasicDBObject();
                updateObj.put("$set", newDocument);

                table1.update(query, updateObj);

                //insert into acquried games collection
                BasicDBObject document = new BasicDBObject();
                document.put("user_name", user_name);
                document.put("game_name", game_name);
                table3.insert(document);
            }
            else{
                System.out.println("Not enough money");
            }
            res=0;
        }
        catch (Exception e){
            throw e;
        }
        return res;
    }

    public Integer createArticles(String name, String description, Integer creatorId) throws Exception{
        Integer res;

        try{
                DB db = mongo.getDB("EngineDB");
                DBCollection table = db.getCollection("Articles");
                BasicDBObject document = new BasicDBObject();
                document.put("name", name);
                document.put("description", description);
                document.put("creatorId", creatorId);
                table.insert(document);
                res = 0;
        }
        catch (Exception e){
            throw e;
        }
        return res;
    }

    public Integer updateArticles(String name, String upname, String description) throws Exception{
        Integer res;

        try{
                DB db = mongo.getDB("EngineDB");
                DBCollection table = db.getCollection("Articles");

                BasicDBObject query = new BasicDBObject();
                query.put("name", name);

                BasicDBObject newDocument = new BasicDBObject();
                newDocument.put("name", upname);
                newDocument.put("description",description);

                BasicDBObject updateObj = new BasicDBObject();
                updateObj.put("$set", newDocument);

                table.update(query, updateObj);
                res = 0;
        }
        catch (Exception e){
            throw e;
        }
        return res;
    }

    public Integer deleteArticles(String name) throws  Exception{
        Integer res;

        try{
            try{
                DB db = mongo.getDB("EngineDB");
                DBCollection table = db.getCollection("Articles");

                BasicDBObject searchQuery = new BasicDBObject();
                searchQuery.put("name", name);

                table.remove(searchQuery);
                res = 0;
            }
            catch (Exception e){
                throw e;
            }
        }
        catch (Exception e){
            throw e;
        }
        return res;
    }

    public void viewArticles(String name) throws Exception{
        try{
            DB db = mongo.getDB("EngineDB");
            DBCollection table = db.getCollection("Articles");

            BasicDBObject searchQuery = new BasicDBObject();
            searchQuery.put("name", name);

            DBCursor cursor = table.find(searchQuery);

            while (cursor.hasNext()) {
                System.out.println(cursor.next());
                jedis.set(String.valueOf(cursor.next().hashCode()), String.valueOf(cursor.next()));
            }
        }
        catch (Exception e){
            throw e;
        }
    }

    public Integer createDevelopers(String name, String description) throws Exception{
        Integer res;

        try{
            DB db = mongo.getDB("EngineDB");
            DBCollection table = db.getCollection("developers");
            BasicDBObject document = new BasicDBObject();
            document.put("name", name);
            document.put("description", description);
            table.insert(document);
            res = 0;
        }
        catch (Exception e){
            throw e;
        }
        return res;
    }

    public Integer updateDevelopers(String name, String upname, String description ) throws Exception{
        Integer res;

        try{
            DB db = mongo.getDB("EngineDB");
            DBCollection table = db.getCollection("developers");

            BasicDBObject query = new BasicDBObject();
            query.put("name", name);

            BasicDBObject newDocument = new BasicDBObject();
            newDocument.put("name", upname);
            newDocument.put("description",description);

            BasicDBObject updateObj = new BasicDBObject();
            updateObj.put("$set", newDocument);

            table.update(query, updateObj);
            res = 0;
        }
        catch (Exception e){
            throw e;
        }
        return res;
    }

    public Integer deleteDevelopers(String name) throws Exception{
        Integer res;

        try{
            DB db = mongo.getDB("EngineDB");
            DBCollection table = db.getCollection("developers");

            BasicDBObject searchQuery = new BasicDBObject();
            searchQuery.put("name", name);

            table.remove(searchQuery);
            res = 0;
        }
        catch (Exception e){
            throw e;
        }
        return res;
    }

    public void viewDevelopers(String name) throws Exception{
        try{
            DB db = mongo.getDB("EngineDB");
            DBCollection table = db.getCollection("developers");

            BasicDBObject searchQuery = new BasicDBObject();
            searchQuery.put("name", name);

            DBCursor cursor = table.find(searchQuery);

            while (cursor.hasNext()) {
                System.out.println(cursor.next());
                jedis.set(String.valueOf(cursor.next().hashCode()), String.valueOf(cursor.next()));
            }
        }
        catch (Exception e){
            throw e;
        }
    }

    public Integer createEvent( String name,Integer authorId, String content,
                                        Integer rating,  Date creationDate,
                                        Integer gameId) throws Exception{
        Integer res;
        try{
            DB db = mongo.getDB("EngineDB");
            DBCollection table = db.getCollection("Events");
            BasicDBObject document = new BasicDBObject();
            document.put("name", name);
            document.put("authorId", authorId);
            document.put("content", content);
            document.put("rating", rating);
            document.put("creationDate",creationDate);
            document.put("gameId",gameId);
            table.insert(document);
            res = 0;
        }
        catch (Exception e){
            throw e;
        }
        return res;
    }

    public Integer updateEvent (String name, String upname, String content,
                                               Integer rating,  Date creationDate) throws Exception{
        Integer res;

        try{
            DB db = mongo.getDB("EngineDB");
            DBCollection table = db.getCollection("Events");

            BasicDBObject query = new BasicDBObject();
            query.put("name", name);

            BasicDBObject newDocument = new BasicDBObject();
            newDocument.put("name", upname);
            newDocument.put("content",content);
            newDocument.put("rating",rating);
            newDocument.put("creationDate",creationDate);

            BasicDBObject updateObj = new BasicDBObject();
            updateObj.put("$set", newDocument);

            table.update(query, updateObj);
            res = 0;
        }
        catch (Exception e){
            throw e;
        }
        return res;
    }

    public Integer deleteEvent(String name) throws  Exception{
        Integer res;

        try{
            DB db = mongo.getDB("EngineDB");
            DBCollection table = db.getCollection("Events");

            BasicDBObject searchQuery = new BasicDBObject();
            searchQuery.put("name", name);

            table.remove(searchQuery);
            res = 0;
        }
        catch (Exception e){
            throw e;
        }
        return res;
    }

    public void viewEvents(String name) throws Exception{

        try{
            DB db = mongo.getDB("EngineDB");
            DBCollection table = db.getCollection("Events");

            BasicDBObject searchQuery = new BasicDBObject();
            searchQuery.put("name", name);

            DBCursor cursor = table.find(searchQuery);

            while (cursor.hasNext()) {
                System.out.println(cursor.next());
                jedis.set(String.valueOf(cursor.next().hashCode()), String.valueOf(cursor.next()));
            }
        }
        catch (Exception e){
            throw e;
        }
    }

    public Integer createCommunity(String name, String description) throws Exception{
        Integer res;

        try{
            DB db = mongo.getDB("EngineDB");
            DBCollection table = db.getCollection("communities");
            BasicDBObject document = new BasicDBObject();
            document.put("name", name);
            document.put("description", description);
            table.insert(document);
            res = 0;
        }
        catch (Exception e){
            throw e;
        }
        return res;
    }

    public Integer addUserToComminuty(String com_name,String user_name) throws Exception{
        Integer res;

        try{
            DB db = mongo.getDB("EngineDB");
            DBCollection table = db.getCollection("users_communities");
            BasicDBObject document = new BasicDBObject();
            document.put("user_name", user_name);
            document.put("com_name", com_name);
            table.insert(document);
            res = 0;
        }
        catch (Exception e){
            throw e;
        }
        return res;
    }

    public Integer deleteUserFromCommunity(String com_name,String user_name) throws  Exception{
        Integer res;

        try{
            DB db = mongo.getDB("EngineDB");
            DBCollection table = db.getCollection("users_communities");

            BasicDBObject searchQuery = new BasicDBObject();
            searchQuery.put("user_name", user_name);

            table.remove(searchQuery);
            res = 0;
        }
        catch (Exception e){
            throw e;
        }
        return res;
    }

    public void viewUsersComminities(String name) throws Exception{

        try{
            DB db = mongo.getDB("EngineDB");
            DBCollection table = db.getCollection("users_communities");

            BasicDBObject searchQuery = new BasicDBObject();
            searchQuery.put("name", name);

            DBCursor cursor = table.find(searchQuery);

            while (cursor.hasNext()) {
                System.out.println(cursor.next());
                jedis.set(String.valueOf(cursor.next().hashCode()), String.valueOf(cursor.next()));
            }
        }
        catch (Exception e){
            throw e;
        }
    }

    public Integer updateCommunity(String name, String upname, String description
                                   ) throws Exception{
        Integer res;

        try{
            DB db = mongo.getDB("EngineDB");
            DBCollection table = db.getCollection("communities");

            BasicDBObject query = new BasicDBObject();
            query.put("name", name);

            BasicDBObject newDocument = new BasicDBObject();
            newDocument.put("name", upname);
            newDocument.put("description",description);

            BasicDBObject updateObj = new BasicDBObject();
            updateObj.put("$set", newDocument);

            table.update(query, updateObj);
            res = 0;
        }
        catch (Exception e){
            throw e;
        }
        return res;
    }

    public Integer deleteCommunity(String name) throws Exception{
        Integer res;

        try{
            DB db = mongo.getDB("EngineDB");
            DBCollection table1 = db.getCollection("communities");
            DBCollection table2 = db.getCollection("users_communities");

            BasicDBObject searchQuery = new BasicDBObject();
            searchQuery.put("name", name);

            BasicDBObject searchQuery2 = new BasicDBObject();
            searchQuery.put("com_name", name);

            table1.remove(searchQuery);
            table2.remove(searchQuery);
            res = 0;
        }
        catch (Exception e){
            throw e;
        }
        return res;
    }

    synchronized public void viewCommunities(String name) throws Exception {
        try {
            try{
                DB db = mongo.getDB("EngineDB");
                DBCollection table = db.getCollection("communities");

                BasicDBObject searchQuery = new BasicDBObject();
                searchQuery.put("name", name);

                DBCursor cursor = table.find(searchQuery);

                while (cursor.hasNext()) {
                    System.out.println(cursor.next());
                    jedis.set(String.valueOf(cursor.next().hashCode()), String.valueOf(cursor.next()));
                }
            }
            catch (Exception e){
                throw e;
            }
        } catch (Exception e) {
            throw e;
        }
    }

    public Integer createGame(String name, Integer cost, String description, Blob trailer, String genre,
                                         Integer developerId, Date releaseDate, Integer sizeBytes
                              )
                                            throws Exception{
        Integer res;

        try{
            DB db = mongo.getDB("EngineDB");
            DBCollection table = db.getCollection("games");
            BasicDBObject document = new BasicDBObject();
            document.put("name", name);
            document.put("cost",cost);
            document.put("description", description);
            document.put("trailer", trailer);
            document.put("developerId", developerId);
            document.put("releaseDate", releaseDate);
            document.put("sizeBytes", sizeBytes);
            document.put("genre", genre);

            table.insert(document);
            res = 0;
        }
        catch (Exception e){
            throw e;
        }
        return res;
    }

    public Integer updateGame(String name, String upname, Integer cost, String description, Blob trailer,
                                     Integer sizeBytes) throws  Exception{
        Integer res;

        try{
            DB db = mongo.getDB("EngineDB");
            DBCollection table = db.getCollection("games");

            BasicDBObject query = new BasicDBObject();
            query.put("name", name);

            BasicDBObject newDocument = new BasicDBObject();
            newDocument.put("name", upname);
            newDocument.put("cost", cost);
            newDocument.put("description",description);
            newDocument.put("trailer", trailer);
            newDocument.put("sizeBytes", sizeBytes);

            BasicDBObject updateObj = new BasicDBObject();
            updateObj.put("$set", newDocument);

            table.update(query, updateObj);
            res = 0;
        }
        catch (Exception e){
             throw e;
        }
        return res;
    }

    public Integer deleteGame(String name) throws Exception{
        Integer res;

        try{
            DB db = mongo.getDB("EngineDB");
            DBCollection table1 = db.getCollection("games");

            BasicDBObject searchQuery = new BasicDBObject();
            searchQuery.put("name", name);

            table1.remove(searchQuery);

            res = 0;
        }
        catch (Exception e){
            throw e;
        }
        return res;
    }

    public void viewGames(String name) throws Exception{
        try{
            DB db = mongo.getDB("EngineDB");
            DBCollection table = db.getCollection("games");

            BasicDBObject searchQuery = new BasicDBObject();
            searchQuery.put("name", name);

            DBCursor cursor = table.find(searchQuery);

            while (cursor.hasNext()) {
                System.out.println(cursor.next());
                jedis.set(String.valueOf(cursor.next().hashCode()), String.valueOf(cursor.next()));
            }
        }
        catch (Exception e){
            throw e;
        }
    }

    public void seeAcquiredGames() throws Exception{
        try{
            DB db = mongo.getDB("EngineDB");
            DBCollection table = db.getCollection("acquiredGames");

            BasicDBObject searchQuery = new BasicDBObject();

            DBCursor cursor = table.find(searchQuery);

            while (cursor.hasNext()) {
                System.out.println(cursor.next());
                jedis.set(String.valueOf(cursor.next().hashCode()),String.valueOf(cursor.next()));
            }
        }
        catch (Exception e){
            throw e;
        }
    }
}
