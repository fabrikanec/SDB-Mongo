import com.mongodb.*;
import redis.clients.jedis.Jedis;

import java.sql.*;
import java.sql.Date;
import java.util.*;

public class EngineDBLibrary {
    private MongoClient mongo;
    private DB db;
    private Jedis jedis;

    EngineDBLibrary(String url, Integer port) {
        this.jedis = new Jedis(url);
        this.mongo = new MongoClient(url, port);
        this.db = mongo.getDB("EngineDB");
    }

    public Integer createUser(String nickName) throws Exception {
        Integer res;
        try {
            if (nickName.length() > 20)
                res = 1;
            else {

                DBCollection table = db.getCollection("users");
                BasicDBObject document = new BasicDBObject();
                document.put("name", nickName);
                document.put("money", 0);
                table.insert(document);
                res = 0;
            }
        } catch (Exception e) {
            throw e;
        }
        return res;
    }

    public Integer updateUser(String nickName, String upnickName, Integer userMoney) throws Exception {
        Integer res;

        try {
            if ((nickName.length() > 20) || (userMoney < 0))
                res = 1;
            else {
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
        } catch (Exception e) {
            throw e;
        }
        return res;
    }

    public Integer deleteUser(String nickName) throws Exception {
        Integer res;

        try {
            DBCollection table = db.getCollection("users");

            BasicDBObject searchQuery = new BasicDBObject();
            searchQuery.put("name", nickName);

            table.remove(searchQuery);
            res = 0;
        } catch (Exception e) {
            throw e;
        }
        return res;
    }

    public void viewUsers(String name) throws Exception {
        try {
            DBCollection table = db.getCollection("users");
            BasicDBObject searchQuery = new BasicDBObject();
            searchQuery.put("name", name);
            DBCursor cursor = table.find(searchQuery);

            while (cursor.hasNext()) {
                DBObject user = cursor.next();
                System.out.println(user);
                jedis.set(String.valueOf(user.hashCode()), String.valueOf(user));
            }
        } catch (Exception e) {
            throw e;
        }
    }

    public Integer updateUserMoney(String nickName, Integer userMoney) throws Exception {
        Integer res;

        try {
            if ((nickName.length() > 20) || (userMoney < 0))
                res = 1;
            else {
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
        } catch (Exception e) {
            throw e;
        }
        return res;
    }

    public Integer findmusicsByGenre(String genre) throws Exception {
        Integer res;

        try {
            DBCollection table = db.getCollection("musics");
            BasicDBObject searchQuery = new BasicDBObject();
            searchQuery.put("genre", genre);
            DBCursor cursor = table.find(searchQuery);

            while (cursor.hasNext()) {
                System.out.println(cursor.next());
                jedis.set(String.valueOf(cursor.next().hashCode()), String.valueOf(cursor.next()));
            }
            res = 0;
        } catch (Exception e) {
            throw e;
        }
        return res;
    }

    public Integer buymusic(String userName, String musicName) throws Exception {
        Integer res;

        try {
            DBCollection table1 = db.getCollection("users");
            DBCollection table2 = db.getCollection("musics");
            DBCollection table3 = db.getCollection("acquiredmusics");

            //get money of the user
            BasicDBObject searchQuery = new BasicDBObject();
            searchQuery.put("name", userName);
            DBObject result = table1.findOne(searchQuery);
            Integer money = Integer.valueOf(result.get("money").toString());

            //get cost of the music
            BasicDBObject searchQuery2 = new BasicDBObject();
            searchQuery2.put("name", musicName);
            DBObject result2 = table2.findOne(searchQuery2);
            Integer cost = Integer.valueOf(result2.get("cost").toString());

            if (cost <= money) {
                // update user money
                BasicDBObject query = new BasicDBObject();
                query.put("name", userName);
                BasicDBObject newDocument = new BasicDBObject();
                newDocument.put("money", money - cost);
                BasicDBObject updateObj = new BasicDBObject();
                updateObj.put("$set", newDocument);
                table1.update(query, updateObj);

                //insert into acquried musics collection
                BasicDBObject document = new BasicDBObject();
                document.put("userName", userName);
                document.put("musicName", musicName);
                table3.insert(document);
            } else {
                System.out.println("Not enough money");
            }
            res = 0;
        } catch (Exception e) {
            throw e;
        }
        return res;
    }

    public Integer createArticles(String name, String description, Integer creatorId) throws Exception {
        Integer res;

        try {
            DBCollection table = db.getCollection("Articles");
            BasicDBObject document = new BasicDBObject();
            document.put("name", name);
            document.put("description", description);
            document.put("creatorId", creatorId);
            table.insert(document);
            res = 0;
        } catch (Exception e) {
            throw e;
        }
        return res;
    }

    public Integer updateArticles(String name, String upname, String description) throws Exception {
        Integer res;

        try {
            DBCollection table = db.getCollection("Articles");
            BasicDBObject query = new BasicDBObject();
            query.put("name", name);
            BasicDBObject newDocument = new BasicDBObject();
            newDocument.put("name", upname);
            newDocument.put("description", description);
            BasicDBObject updateObj = new BasicDBObject();
            updateObj.put("$set", newDocument);

            table.update(query, updateObj);
            res = 0;
        } catch (Exception e) {
            throw e;
        }
        return res;
    }

    public Integer deleteArticles(String name) throws Exception {
        Integer res;

        try {
            DBCollection table = db.getCollection("Articles");
            BasicDBObject searchQuery = new BasicDBObject();
            searchQuery.put("name", name);

            table.remove(searchQuery);
            res = 0;
        } catch (Exception e) {
            throw e;
        }
        return res;
    }

    public void viewArticles(String name) throws Exception {
        try {
            DBCollection table = db.getCollection("Articles");
            BasicDBObject searchQuery = new BasicDBObject();
            searchQuery.put("name", name);
            DBCursor cursor = table.find(searchQuery);

            while (cursor.hasNext()) {
                DBObject article = cursor.next();
                System.out.println(article);
                jedis.set(String.valueOf(article.hashCode()), String.valueOf(article));
            }
        } catch (Exception e) {
            throw e;
        }
    }

    public Integer createDevelopers(String name, String description) throws Exception {
        Integer res;

        try {
            DBCollection table = db.getCollection("developers");
            BasicDBObject document = new BasicDBObject();
            document.put("name", name);
            document.put("description", description);
            table.insert(document);
            res = 0;
        } catch (Exception e) {
            throw e;
        }
        return res;
    }

    public Integer updateDevelopers(String name, String upname, String description) throws Exception {
        Integer res;

        try {
            DBCollection table = db.getCollection("developers");
            BasicDBObject query = new BasicDBObject();
            query.put("name", name);
            BasicDBObject newDocument = new BasicDBObject();
            newDocument.put("name", upname);
            newDocument.put("description", description);
            BasicDBObject updateObj = new BasicDBObject();
            updateObj.put("$set", newDocument);

            table.update(query, updateObj);
            res = 0;
        } catch (Exception e) {
            throw e;
        }
        return res;
    }

    public Integer deleteDevelopers(String name) throws Exception {
        Integer res;

        try {
            DBCollection table = db.getCollection("developers");
            BasicDBObject searchQuery = new BasicDBObject();
            searchQuery.put("name", name);

            table.remove(searchQuery);
            res = 0;
        } catch (Exception e) {
            throw e;
        }
        return res;
    }

    public void viewDevelopers(String name) throws Exception {
        try {
            DBCollection table = db.getCollection("developers");
            BasicDBObject searchQuery = new BasicDBObject();
            searchQuery.put("name", name);
            DBCursor cursor = table.find(searchQuery);

            while (cursor.hasNext()) {
                DBObject developer = cursor.next();
                System.out.println(developer);
                jedis.set(String.valueOf(developer.hashCode()), String.valueOf(developer));
            }
        } catch (Exception e) {
            throw e;
        }
    }

    public Integer createEvent(String name, Integer authorId, String content,
                               Integer rating, Date creationDate,
                               Integer musicId) throws Exception {
        Integer res;
        try {
            DBCollection table = db.getCollection("events");
            BasicDBObject document = new BasicDBObject();
            document.put("name", name);
            document.put("authorId", authorId);
            document.put("content", content);
            document.put("rating", rating);
            document.put("creationDate", creationDate);
            document.put("musicId", musicId);
            table.insert(document);
            res = 0;
        } catch (Exception e) {
            throw e;
        }
        return res;
    }

    public Integer updateEvent(String name, String upname, String content,
                               Integer rating, Date creationDate) throws Exception {
        Integer res;

        try {
            DBCollection table = db.getCollection("Events");
            BasicDBObject query = new BasicDBObject();
            query.put("name", name);
            BasicDBObject newDocument = new BasicDBObject();
            newDocument.put("name", upname);
            newDocument.put("content", content);
            newDocument.put("rating", rating);
            newDocument.put("creationDate", creationDate);
            BasicDBObject updateObj = new BasicDBObject();
            updateObj.put("$set", newDocument);

            table.update(query, updateObj);
            res = 0;
        } catch (Exception e) {
            throw e;
        }
        return res;
    }

    public Integer deleteEvent(String name) throws Exception {
        Integer res;

        try {
            DBCollection table = db.getCollection("Events");
            BasicDBObject searchQuery = new BasicDBObject();
            searchQuery.put("name", name);

            table.remove(searchQuery);
            res = 0;
        } catch (Exception e) {
            throw e;
        }
        return res;
    }

    public void viewEvents(String name) throws Exception {

        try {
            DBCollection table = db.getCollection("Events");
            BasicDBObject searchQuery = new BasicDBObject();
            searchQuery.put("name", name);
            DBCursor cursor = table.find(searchQuery);

            while (cursor.hasNext()) {
                DBObject event = cursor.next();
                System.out.println(event);
                jedis.set(String.valueOf(event.hashCode()), String.valueOf(event));
            }
        } catch (Exception e) {
            throw e;
        }
    }

    public Integer createCommunity(String name, String description) throws Exception {
        Integer res;

        try {
            DBCollection table = db.getCollection("communities");
            BasicDBObject document = new BasicDBObject();
            document.put("name", name);
            document.put("description", description);
            table.insert(document);
            res = 0;
        } catch (Exception e) {
            throw e;
        }
        return res;
    }

    public Integer addUserToComminuty(String comName, String userName) throws Exception {
        Integer res;

        try {
            DBCollection table = db.getCollection("users_communities");
            BasicDBObject document = new BasicDBObject();
            document.put("userName", userName);
            document.put("comName", comName);
            table.insert(document);
            res = 0;
        } catch (Exception e) {
            throw e;
        }
        return res;
    }

    public Integer deleteUserFromCommunity(String comName, String userName) throws Exception {
        Integer res;

        try {
            DBCollection table = db.getCollection("users_communities");
            BasicDBObject searchQuery = new BasicDBObject();
            searchQuery.put("userName", userName);

            table.remove(searchQuery);
            res = 0;
        } catch (Exception e) {
            throw e;
        }
        return res;
    }

    public void viewUsersComminities(String name) throws Exception {

        try {
            DBCollection table = db.getCollection("users_communities");
            BasicDBObject searchQuery = new BasicDBObject();
            searchQuery.put("name", name);
            DBCursor cursor = table.find(searchQuery);

            while (cursor.hasNext()) {
                DBObject usersCommunity = cursor.next();
                System.out.println(usersCommunity);
                jedis.set(String.valueOf(usersCommunity.hashCode()), String.valueOf(usersCommunity));
            }
        } catch (Exception e) {
            throw e;
        }
    }

    public Integer updateCommunity(String name, String upname, String description) throws Exception {
        Integer res;

        try {
            DBCollection table = db.getCollection("communities");
            BasicDBObject query = new BasicDBObject();
            query.put("name", name);
            BasicDBObject newDocument = new BasicDBObject();
            newDocument.put("name", upname);
            newDocument.put("description", description);
            BasicDBObject updateObj = new BasicDBObject();
            updateObj.put("$set", newDocument);

            table.update(query, updateObj);
            res = 0;
        } catch (Exception e) {
            throw e;
        }
        return res;
    }

    public Integer deleteCommunity(String name) throws Exception {
        Integer res;

        try {
            DBCollection table = db.getCollection("communities");
            BasicDBObject searchQuery = new BasicDBObject();
            searchQuery.put("name", name);
            table.remove(searchQuery);
            table = db.getCollection("users_communities");
            searchQuery = new BasicDBObject();
            searchQuery.put("comName", name);
            table.remove(searchQuery);

            res = 0;
        } catch (Exception e) {
            throw e;
        }
        return res;
    }

    public void viewCommunities(String name) throws Exception {
        try {
            DBCollection table = db.getCollection("communities");
            BasicDBObject searchQuery = new BasicDBObject();
            searchQuery.put("name", name);
            DBCursor cursor = table.find(searchQuery);

            while (cursor.hasNext()) {
                DBObject community = cursor.next();
                System.out.println(community);
                jedis.set(String.valueOf(community.hashCode()), String.valueOf(community));
            }
        } catch (Exception e) {
            throw e;
        }
    }

    public Integer createmusic(String name, Integer cost, String description, Blob trailer, String genre,
                              Integer developerId, Date releaseDate, Integer sizeBytes)
            throws Exception {
        Integer res;

        try {
            DBCollection table = db.getCollection("musics");
            BasicDBObject document = new BasicDBObject();
            document.put("name", name);
            document.put("cost", cost);
            document.put("description", description);
            document.put("trailer", trailer);
            document.put("developerId", developerId);
            document.put("releaseDate", releaseDate);
            document.put("sizeBytes", sizeBytes);
            document.put("genre", genre);

            table.insert(document);
            res = 0;
        } catch (Exception e) {
            throw e;
        }
        return res;
    }

    public Integer updatemusic(String name, String upname, Integer cost, String description, Blob trailer,
                              Integer sizeBytes) throws Exception {
        Integer res;

        try {
            DBCollection table = db.getCollection("musics");
            BasicDBObject query = new BasicDBObject();
            query.put("name", name);
            BasicDBObject newDocument = new BasicDBObject();
            newDocument.put("name", upname);
            newDocument.put("cost", cost);
            newDocument.put("description", description);
            newDocument.put("trailer", trailer);
            newDocument.put("sizeBytes", sizeBytes);
            BasicDBObject updateObj = new BasicDBObject();
            updateObj.put("$set", newDocument);

            table.update(query, updateObj);
            res = 0;
        } catch (Exception e) {
            throw e;
        }
        return res;
    }

    public Integer deletemusic(String name) throws Exception {
        Integer res;

        try {
            DBCollection table = db.getCollection("musics");
            BasicDBObject searchQuery = new BasicDBObject();
            searchQuery.put("name", name);
            table.remove(searchQuery);

            res = 0;
        } catch (Exception e) {
            throw e;
        }
        return res;
    }

    public void viewmusics(String name) throws Exception {
        try {
            DBCollection table = db.getCollection("musics");
            BasicDBObject searchQuery = new BasicDBObject();
            searchQuery.put("name", name);
            DBCursor cursor = table.find(searchQuery);

            while (cursor.hasNext()) {
                DBObject music = cursor.next();
                System.out.println(music);
                jedis.set(String.valueOf(music.hashCode()), String.valueOf(music));
            }
        } catch (Exception e) {
            throw e;
        }
    }

    public void seeAcquiredmusics() throws Exception {
        try {
            DBCollection table = db.getCollection("acquiredmusics");
            BasicDBObject searchQuery = new BasicDBObject();
            DBCursor cursor = table.find(searchQuery);

            while (cursor.hasNext()) {
                System.out.println(cursor.next());
                jedis.set(String.valueOf(cursor.next().hashCode()), String.valueOf(cursor.next()));
            }
        } catch (Exception e) {
            throw e;
        }
    }
}
