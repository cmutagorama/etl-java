package com.example.postgresimport;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

public class PostgresImport {
    public static Connection connectToDB() throws Exception {
//        DriverManager.registerDriver(new com.mysql.jdbc.Driver());
//        Class.forName("com.mysql.jdbc.Driver");
        String postgresUrl = "jdbc:postgresql://localhost/test";
        Connection con = DriverManager.getConnection(postgresUrl, "postgres", "");
        System.out.println("Connection established......");
        return con;
    }

    public static void main(String[] args) {
        JSONParser jsonParser = new JSONParser();
        File data = new File("resources/query2_ref.txt");
        File popularTags = new File("resources/popular_hashtags.txt");
        try {
            List<String> languages = Arrays.asList("ar", "en", "fr", "in", "pt", "es", "tr", "ja");
            List<Long> ids = new ArrayList<>();
            List<Long> user_ids = new ArrayList<>();
            List<String> excluded_hashtags = new ArrayList<>();
            List<JSONObject> hashtag_table = new ArrayList<>();

            /* Loading excluded hashtags in a list */
            try (BufferedReader br = new BufferedReader(new FileReader(popularTags))) {
                String line;
                while ((line = br.readLine()) != null) {
                    excluded_hashtags.add(line);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }

            Connection con = connectToDB();

            try (BufferedReader br = new BufferedReader(new FileReader(data))) {
                String line;
                while ((line = br.readLine()) != null) {
                    Long reply_to_tweet_id = null;
                    Long reply_to_user_id = null;
                    String tweet_type = null;
                    Long retweet_to_tweet_id = null;
                    Long retweet_to_user_id = null;

                    JSONObject jsonObject = (JSONObject) jsonParser.parse(line);
                    Long id = (Long) jsonObject.get("id");
                    String id_str = (String) jsonObject.get("id_str");
                    String createdAt = (String) jsonObject.get("created_at");
                    String text = (String) jsonObject.get("text");
                    JSONObject entities = (JSONObject) jsonObject.get("entities");
                    List<JSONObject> hashtags = (List<JSONObject>) entities.get("hashtags");
                    JSONObject user = (JSONObject) jsonObject.get("user");
                    Long user_id = (Long) user.get("id");
                    String user_id_str = (String) user.get("id_str");
                    String user_screen_name = (String) user.get("screen_name");
                    String user_description = (String) user.get("description");
                    JSONObject retweeted_status = (JSONObject) jsonObject.get("retweeted_status");
                    Long in_reply_to_user_id = (Long) jsonObject.get("in_reply_to_user_id");
                    String in_reply_to_screen_name = (String) jsonObject.get("in_reply_to_screen_name");
                    Long in_reply_to_status_id = (Long) jsonObject.get("in_reply_to_status_id");
                    String lang = (String) jsonObject.get("lang");

                    /* Filtering out malformed tweets */
                    if (id == null || id_str == null || createdAt == null || text == null || hashtags == null || user_id == null || user_id_str == null || ids.contains(id) || !languages.contains(lang)) {
                        continue;
                    } else if (hashtags != null && hashtags.size() == 0) {
                        continue;
                    } else {
                        ids.add(id);
                        if (in_reply_to_user_id != null && in_reply_to_status_id != null) {
                            tweet_type = "reply";
                            reply_to_tweet_id = in_reply_to_status_id;
                            reply_to_user_id = in_reply_to_user_id;
                        }

                        if (retweeted_status != null) {
                            tweet_type = "retweet";
                            retweet_to_tweet_id = (Long) retweeted_status.get("id");
                            JSONObject retweetToUser = (JSONObject) retweeted_status.get("user");
                            retweet_to_user_id = (Long) retweetToUser.get("id");
                        }

                        List<String> tags = new ArrayList<>();
                        for (JSONObject tag: hashtags) {
                            String txt = (String) tag.get("text");
                            if (!excluded_hashtags.contains(txt)) {
                                tags.add(txt);

                                JSONObject ht = new JSONObject();
                                ht.put("tweet_id", id);
                                ht.put("user_id", user_id);
                                ht.put("hashtag_name", txt);

                                if (!hashtag_table.contains(ht)) {
                                    hashtag_table.add(ht);
                                    PreparedStatement hashStmt = con.prepareStatement("INSERT INTO hashtag (tweet_id, user_id, hashtag_name) values (?, ?, ?)");
                                    hashStmt.setLong(1, Long.parseLong(String.valueOf(id)));
                                    hashStmt.setLong(2, Long.parseLong(String.valueOf(user_id)));
                                    hashStmt.setString(3, txt);
                                    hashStmt.executeUpdate();
                                }
                            }
                        }

                        String hash_tags = String. join(",", tags);

                        DateFormat formatter = new SimpleDateFormat("E MMM dd HH:mm:ss Z yyyy");
                        Date date = (Date)formatter.parse(createdAt);

                        PreparedStatement tweetStmt = con.prepareStatement("INSERT INTO tweet values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
                        tweetStmt.setLong(1, id);
                        tweetStmt.setDate(2, new java.sql.Date(date.getTime()));
                        tweetStmt.setString(3, text);
                        tweetStmt.setLong(4, user_id);
                        tweetStmt.setString(5, user_screen_name);
                        tweetStmt.setString(6, user_description);
                        tweetStmt.setString(7, tweet_type);

                        if (reply_to_tweet_id != null) {
                            tweetStmt.setLong(8, reply_to_tweet_id);
                        } else {
                            tweetStmt.setNull(8, java.sql.Types.NULL);
                        }

                        if (reply_to_user_id != null) {
                            tweetStmt.setLong(9, reply_to_user_id);
                        } else {
                            tweetStmt.setNull(9, java.sql.Types.NULL);
                        }

                        if (retweet_to_tweet_id != null) {
                            tweetStmt.setLong(10, retweet_to_tweet_id);
                        } else {
                            tweetStmt.setNull(10, java.sql.Types.NULL);
                        }

                        if (retweet_to_user_id != null) {
                            tweetStmt.setLong(11, retweet_to_user_id);
                        } else {
                            tweetStmt.setNull(11, java.sql.Types.NULL);
                        }

                        tweetStmt.setString(12, hash_tags);
                        tweetStmt.executeUpdate();

                        PreparedStatement userStmt = con.prepareStatement("INSERT INTO user_account values (?, ?, ?, ?, ?)");
                        if (!user_ids.contains(user_id)) {
                            user_ids.add(user_id);
                            String screen_name = (String) user.get("screen_name");
                            String description = (String) user.get("description");
                            String name = (String) user.get("name");
                            String userCreatedAt = (String) user.get("created_at");
                            Date user_created_at = (Date)formatter.parse(userCreatedAt);

                            userStmt.setLong(1, user_id);
                            userStmt.setString(2, screen_name);
                            userStmt.setString(3, description);
                            userStmt.setString(4, name);
                            userStmt.setDate(5, new java.sql.Date(user_created_at.getTime()));
                            userStmt.executeUpdate();
                        }

                        if (reply_to_user_id != null && !user_ids.contains(reply_to_user_id)) {
                            user_ids.add(reply_to_user_id);
                            userStmt.setLong(1, reply_to_user_id);
                            userStmt.setString(2, in_reply_to_screen_name);
                            userStmt.setString(3, null);
                            userStmt.setString(4, null);
                            userStmt.setNull(5, java.sql.Types.NULL);
                            userStmt.executeUpdate();
                        }

                        if (retweeted_status != null) {
                            JSONObject retweetToUser = (JSONObject) retweeted_status.get("user");
                            Long retweeet_to_user_id = (Long) retweetToUser.get("id");

                            if (!user_ids.contains(retweeet_to_user_id)) {
                                user_ids.add(retweeet_to_user_id);
                                String screen_name = (String) retweetToUser.get("screen_name");
                                String description = (String) retweetToUser.get("description");
                                String name = (String) retweetToUser.get("name");
                                String userCreatedAt = (String) retweetToUser.get("created_at");
                                Date user_created_at = (Date)formatter.parse(userCreatedAt);

                                userStmt.setLong(1, retweeet_to_user_id);
                                userStmt.setString(2, screen_name);
                                userStmt.setString(3, description);
                                userStmt.setString(4, name);
                                userStmt.setDate(5, new java.sql.Date(user_created_at.getTime()));
                                userStmt.executeUpdate();
                            }
                        }
                    }
                }

                System.out.println(ids.size());
                System.out.println(hashtag_table.size());
                System.out.println(user_ids.size());
            } catch (IOException e) {
                e.printStackTrace();
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }  catch (Exception e) {
            e.printStackTrace();
        }
    }
}
