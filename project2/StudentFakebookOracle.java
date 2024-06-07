package project2;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

/*
    The StudentFakebookOracle class is derived from the FakebookOracle class and implements
    the abstract query functions that investigate the database provided via the <connection>
    parameter of the constructor to discover specific information.
*/
public final class StudentFakebookOracle extends FakebookOracle {
    // [Constructor]
    // REQUIRES: <connection> is a valid JDBC connection
    public StudentFakebookOracle(Connection connection) {
        oracle = connection;
    }

    @Override
    // Query 0
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the total number of users for which a birth month is listed
    //        (B) Find the birth month in which the most users were born
    //        (C) Find the birth month in which the fewest users (at least one) were born
    //        (D) Find the IDs, first names, and last names of users born in the month
    //            identified in (B)
    //        (E) Find the IDs, first names, and last name of users born in the month
    //            identified in (C)
    //
    // This query is provided to you completed for reference. Below you will find the appropriate
    // mechanisms for opening up a statement, executing a query, walking through results, extracting
    // data, and more things that you will need to do for the remaining nine queries
    public BirthMonthInfo findMonthOfBirthInfo() throws SQLException {
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll,
                FakebookOracleConstants.ReadOnly)) {
            // Step 1
            // ------------
            // * Find the total number of users with birth month info
            // * Find the month in which the most users were born
            // * Find the month in which the fewest (but at least 1) users were born
            ResultSet rst = stmt.executeQuery(
                    "SELECT COUNT(*) AS Birthed, Month_of_Birth " + // select birth months and number of uses with that birth month
                            "FROM " + UsersTable + " " + // from all users
                            "WHERE Month_of_Birth IS NOT NULL " + // for which a birth month is available
                            "GROUP BY Month_of_Birth " + // group into buckets by birth month
                            "ORDER BY Birthed DESC, Month_of_Birth ASC"); // sort by users born in that month, descending; break ties by birth month

            int mostMonth = 0;
            int leastMonth = 0;
            int total = 0;
            while (rst.next()) { // step through result rows/records one by one
                if (rst.isFirst()) { // if first record
                    mostMonth = rst.getInt(2); //   it is the month with the most
                }
                if (rst.isLast()) { // if last record
                    leastMonth = rst.getInt(2); //   it is the month with the least
                }
                total += rst.getInt(1); // get the first field's value as an integer
            }
            BirthMonthInfo info = new BirthMonthInfo(total, mostMonth, leastMonth);

            // Step 2
            // ------------
            // * Get the names of users born in the most popular birth month
            rst = stmt.executeQuery(
                    "SELECT User_ID, First_Name, Last_Name " + // select ID, first name, and last name
                            "FROM " + UsersTable + " " + // from all users
                            "WHERE Month_of_Birth = " + mostMonth + " " + // born in the most popular birth month
                            "ORDER BY User_ID"); // sort smaller IDs first

            while (rst.next()) {
                info.addMostPopularBirthMonthUser(new UserInfo(rst.getLong(1), rst.getString(2), rst.getString(3)));
            }

            // Step 3
            // ------------
            // * Get the names of users born in the least popular birth month
            rst = stmt.executeQuery(
                    "SELECT User_ID, First_Name, Last_Name " + // select ID, first name, and last name
                            "FROM " + UsersTable + " " + // from all users
                            "WHERE Month_of_Birth = " + leastMonth + " " + // born in the least popular birth month
                            "ORDER BY User_ID"); // sort smaller IDs first

            while (rst.next()) {
                info.addLeastPopularBirthMonthUser(new UserInfo(rst.getLong(1), rst.getString(2), rst.getString(3)));
            }

            // Step 4
            // ------------
            // * Close resources being used
            rst.close();
            stmt.close(); // if you close the statement first, the result set gets closed automatically

            return info;

        } catch (SQLException e) {
            System.err.println(e.getMessage());
            return new BirthMonthInfo(-1, -1, -1);
        }
    }

    @Override
    // Query 1
    // -----------------------------------------------------------------------------------
    // GOALS: (A) The first name(s) with the most letters
    //        (B) The first name(s) with the fewest letters
    //        (C) The first name held by the most users
    //        (D) The number of users whose first name is that identified in (C)
    public FirstNameInfo findNameInfo() throws SQLException {
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll,
                FakebookOracleConstants.ReadOnly)) {
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                FirstNameInfo info = new FirstNameInfo();
                info.addLongName("Aristophanes");
                info.addLongName("Michelangelo");
                info.addLongName("Peisistratos");
                info.addShortName("Bob");
                info.addShortName("Sue");
                info.addCommonName("Harold");
                info.addCommonName("Jessica");
                info.setCommonNameCount(42);
                return info;
            */
        

            ResultSet rst = stmt.executeQuery(
                "SELECT DISTINCT LENGTH(First_Name) AS flen, First_Name " + 
                "FROM " + UsersTable + " " +
                "ORDER BY flen DESC, First_Name ASC");

            FirstNameInfo info = new FirstNameInfo();

            int mostLetters = 0;
            int leastLetters = 0;
            
            
            if (rst.next()) { // if first record
                mostLetters = rst.getInt(1); //   it is the month with the most
            }
            rst.afterLast();
            if (rst.previous()) { // if last record
                leastLetters = rst.getInt(1); //   it is the month with the least
            }
            
            rst = stmt.executeQuery(
                "SELECT DISTINCT First_Name " + 
                "FROM " + UsersTable + " " +
                "WHERE LENGTH(First_Name) = " + mostLetters + " " +
                "ORDER BY First_Name ASC");

            while (rst.next()) {
                info.addLongName(rst.getString(1));
            }
            
            rst = stmt.executeQuery(
                "SELECT DISTINCT First_Name " + 
                "FROM " + UsersTable + " " +
                "WHERE LENGTH(First_Name) = " + leastLetters + " " +
                "ORDER BY First_Name ASC");

            while (rst.next()) {
                info.addShortName(rst.getString(1));
            }

            
            rst = stmt.executeQuery(
                "SELECT First_Name, COUNT(*) AS count " +
                "FROM " + UsersTable + " " +
                "GROUP BY First_Name " +
                "ORDER BY count DESC, First_Name ASC");
        
            if (rst.next()) { // The first row has the most common name
                int maxCount = rst.getInt("count");
                info.setCommonNameCount(maxCount);
                String commonName = rst.getString("First_Name");
                info.addCommonName(commonName);
    
                // Check if there are other names with the same count
                while (rst.next() && rst.getInt("count") == maxCount) {
                    info.addCommonName(rst.getString("First_Name"));
                }
            }
            
            rst.close();
            stmt.close();

            return info; // placeholder for compilation
        } catch (SQLException e) {
            System.err.println(e.getMessage());
            return new FirstNameInfo();
        }
    }

    @Override
    // Query 2
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the IDs, first names, and last names of users without any friends
    //
    // Be careful! Remember that if two users are friends, the Friends table only contains
    // the one entry (U1, U2) where U1 < U2.
    public FakebookArrayList<UserInfo> lonelyUsers() throws SQLException {
        FakebookArrayList<UserInfo> results = new FakebookArrayList<UserInfo>(", ");
        

        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly)) {
            /*
            EXAMPLE DATA STRUCTURE USAGE
            ============================================
            UserInfo u1 = new UserInfo(15, "Abraham", "Lincoln");
            UserInfo u2 = new UserInfo(39, "Margaret", "Thatcher");
            results.add(u1);
            results.add(u2);
            */

            ResultSet rst = stmt.executeQuery(
                "SELECT USER_ID, FIRST_NAME, LAST_NAME " +
                   "FROM " + UsersTable + " U " + 
                   "WHERE NOT EXISTS ( " +
                   "    SELECT 1 " +
                   "    FROM " + FriendsTable + " F " +
                   "    WHERE U.USER_ID = F.USER1_ID " +
                   "       OR U.USER_ID = F.USER2_ID " +
                   ") " +
                   "ORDER BY USER_ID");
            
            // Iterate through the result set and add users to the results list
            while (rst.next()) {
                Long id = rst.getLong(1);
                String firstName = rst.getString(2);
                String lastName = rst.getString(3);
                UserInfo user = new UserInfo(id, firstName, lastName);
                results.add(user);
            }
            rst.close();
            stmt.close();
        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }

        return results;
    }


    @Override
    // Query 3
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the IDs, first names, and last names of users who no longer live
    //            in their hometown (i.e. their current city and their hometown are different)
    public FakebookArrayList<UserInfo> liveAwayFromHome() throws SQLException {
        FakebookArrayList<UserInfo> results = new FakebookArrayList<UserInfo>(", ");

        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll,
                FakebookOracleConstants.ReadOnly)) {
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                UserInfo u1 = new UserInfo(9, "Meryl", "Streep");
                UserInfo u2 = new UserInfo(104, "Tom", "Hanks");
                results.add(u1);
                results.add(u2);
            */

            ResultSet rst = stmt.executeQuery(
                "SELECT U.USER_ID, U.FIRST_NAME, U.LAST_NAME " +
                "FROM " + UsersTable + " U " +
                "JOIN " + CurrentCitiesTable + " CC ON U.USER_ID = CC.USER_ID " +
                "JOIN " + HometownCitiesTable + " HC ON U.USER_ID = HC.USER_ID " +
                "WHERE CC.CURRENT_CITY_ID != HC.HOMETOWN_CITY_ID " +
                "ORDER BY U.USER_ID");

            // Iterate through the result set and add users to the results list
            while (rst.next()) {
                Long id = rst.getLong("USER_ID");
                String firstName = rst.getString("FIRST_NAME");
                String lastName = rst.getString("LAST_NAME");
                UserInfo user = new UserInfo(id, firstName, lastName);
                results.add(user);
            }

            rst.close();
            stmt.close();


        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }

        return results;
    }

    @Override
    // Query 4
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the IDs, links, and IDs and names of the containing album of the top
    //            <num> photos with the most tagged users
    //        (B) For each photo identified in (A), find the IDs, first names, and last names
    //            of the users therein tagged
    public FakebookArrayList<TaggedPhotoInfo> findPhotosWithMostTags(int num) throws SQLException {
        FakebookArrayList<TaggedPhotoInfo> results = new FakebookArrayList<>("\n");
    
        // Outer try-with-resources for the outer Statement and ResultSet
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly)) {
            
            // Query to fetch the top 'num' photos with the most tags
            String sql = "SELECT P.PHOTO_ID, P.ALBUM_ID, P.PHOTO_LINK, A.ALBUM_NAME, COUNT(T.TAG_SUBJECT_ID) AS count " +
                         "FROM " + PhotosTable + " P " +
                         "JOIN " + AlbumsTable + " A ON P.ALBUM_ID = A.ALBUM_ID " +
                         "JOIN " + TagsTable + " T ON P.PHOTO_ID = T.TAG_PHOTO_ID " +
                         "GROUP BY P.PHOTO_ID, P.ALBUM_ID, P.PHOTO_LINK, A.ALBUM_NAME " +
                         "ORDER BY count DESC, P.PHOTO_ID ASC";
    
            try (ResultSet topPhotosRs = stmt.executeQuery(sql)) {
                while (topPhotosRs.next() && num-- > 0) {
                    // Extract photo information
                    Long photoId = topPhotosRs.getLong("PHOTO_ID");
                    Long albumId = topPhotosRs.getLong("ALBUM_ID");
                    String photoLink = topPhotosRs.getString("PHOTO_LINK");
                    String albumName = topPhotosRs.getString("ALBUM_NAME");
    
                    PhotoInfo photoInfo = new PhotoInfo(photoId, albumId, photoLink, albumName);
                    TaggedPhotoInfo taggedPhotoInfo = new TaggedPhotoInfo(photoInfo);
    
                    // Inner try-with-resources for the inner Statement and ResultSet
                    String taggedUsersSql = "SELECT U.USER_ID, U.FIRST_NAME, U.LAST_NAME " +
                                            "FROM " + UsersTable + " U " +
                                            "JOIN " + TagsTable + " T ON U.USER_ID = T.TAG_SUBJECT_ID " +
                                            "WHERE T.TAG_PHOTO_ID = " + photoId + " " +
                                            "ORDER BY U.USER_ID ASC";
    
                    try (Statement stmt2 = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly);
                         ResultSet taggedUsersRs = stmt2.executeQuery(taggedUsersSql)) {
    
                        // Iterate through the tagged users and add to the taggedPhotoInfo
                        while (taggedUsersRs.next()) {
                            Long userId = taggedUsersRs.getLong("USER_ID");
                            String firstName = taggedUsersRs.getString("FIRST_NAME");
                            String lastName = taggedUsersRs.getString("LAST_NAME");
    
                            UserInfo userInfo = new UserInfo(userId, firstName, lastName);
                            taggedPhotoInfo.addTaggedUser(userInfo);
                        }
                    } catch (SQLException e) {
                        System.err.println("Error executing query for tagged users: " + e.getMessage());
                    }
    
                    results.add(taggedPhotoInfo);
                }
            }
        } catch (SQLException e) {
            System.err.println("Error executing query for top photos: " + e.getMessage());
        }
    
        return results;
    }
    

    @Override
    // Query 5
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the IDs, first names, last names, and birth years of each of the two
    //            users in the top <num> pairs of users that meet each of the following
    //            criteria:
    //              (i) same gender
    //              (ii) tagged in at least one common photo
    //              (iii) difference in birth years is no more than <yearDiff>
    //              (iv) not friends
    //        (B) For each pair identified in (A), find the IDs, links, and IDs and names of
    //            the containing album of each photo in which they are tagged together
    public FakebookArrayList<MatchPair> matchMaker(int num, int yearDiff) throws SQLException {
        FakebookArrayList<MatchPair> results = new FakebookArrayList<>("\n");
    
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll,
                FakebookOracleConstants.ReadOnly)) {
            
            // Query to find potential pairs of users who meet the criteria
            String potentialPairsQuery = 
                "SELECT U1.USER_ID AS USER1_ID, U1.FIRST_NAME AS USER1_FIRST, U1.LAST_NAME AS USER1_LAST, " +
                "U1.YEAR_OF_BIRTH AS USER1_BIRTH_YEAR, U2.USER_ID AS USER2_ID, U2.FIRST_NAME AS USER2_FIRST, " +
                "U2.LAST_NAME AS USER2_LAST, U2.YEAR_OF_BIRTH AS USER2_BIRTH_YEAR, " +
                "COUNT(DISTINCT T1.TAG_PHOTO_ID) AS COMMON_PHOTOS_COUNT " +
                "FROM " + UsersTable + " U1 " +
                "JOIN " + UsersTable + " U2 ON U1.GENDER = U2.GENDER AND U1.USER_ID < U2.USER_ID " +
                "JOIN " + TagsTable + " T1 ON U1.USER_ID = T1.TAG_SUBJECT_ID " +
                "JOIN " + TagsTable + " T2 ON U2.USER_ID = T2.TAG_SUBJECT_ID AND T1.TAG_PHOTO_ID = T2.TAG_PHOTO_ID " +
                "LEFT JOIN " + FriendsTable + " F ON (U1.USER_ID = F.USER1_ID AND U2.USER_ID = F.USER2_ID) " +
                "WHERE F.USER1_ID IS NULL AND F.USER2_ID IS NULL " +
                "AND ABS(U1.YEAR_OF_BIRTH - U2.YEAR_OF_BIRTH) <= " + yearDiff + " " +
                "GROUP BY U1.USER_ID, U1.FIRST_NAME, U1.LAST_NAME, U1.YEAR_OF_BIRTH, " +
                "U2.USER_ID, U2.FIRST_NAME, U2.LAST_NAME, U2.YEAR_OF_BIRTH " +
                "HAVING COUNT(DISTINCT T1.TAG_PHOTO_ID) > 0 " +
                "ORDER BY COMMON_PHOTOS_COUNT DESC, U1.USER_ID ASC, U2.USER_ID ASC ";
    
            try (ResultSet pairsRs = stmt.executeQuery(potentialPairsQuery);
                 Statement stmt2 = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly)) {
    
                int count = 0;
                
                while (pairsRs.next() && count < num) {
                    // Retrieve user details inside the while loop
                    long user1Id = pairsRs.getLong("USER1_ID");
                    String user1FirstName = pairsRs.getString("USER1_FIRST");
                    String user1LastName = pairsRs.getString("USER1_LAST");
                    int user1BirthYear = pairsRs.getInt("USER1_BIRTH_YEAR");
                    UserInfo user1 = new UserInfo(user1Id, user1FirstName, user1LastName);
    
                    long user2Id = pairsRs.getLong("USER2_ID");
                    String user2FirstName = pairsRs.getString("USER2_FIRST");
                    String user2LastName = pairsRs.getString("USER2_LAST");
                    int user2BirthYear = pairsRs.getInt("USER2_BIRTH_YEAR");
                    UserInfo user2 = new UserInfo(user2Id, user2FirstName, user2LastName);
    
                    MatchPair matchPair = new MatchPair(user1, user1BirthYear, user2, user2BirthYear);
    
                    // Query to get the photos in which both users are tagged together
                    String commonPhotosQuery = 
                        "SELECT P.PHOTO_ID, P.PHOTO_LINK, P.ALBUM_ID, A.ALBUM_NAME " +
                        "FROM " + PhotosTable + " P " +
                        "JOIN " + TagsTable + " T1 ON P.PHOTO_ID = T1.TAG_PHOTO_ID " +
                        "JOIN " + TagsTable + " T2 ON P.PHOTO_ID = T2.TAG_PHOTO_ID " +
                        "JOIN " + AlbumsTable + " A ON P.ALBUM_ID = A.ALBUM_ID " +
                        "WHERE T1.TAG_SUBJECT_ID = " + user1Id + " AND T2.TAG_SUBJECT_ID = " + user2Id + " " +
                        "ORDER BY P.PHOTO_ID ASC";
    
                    try (ResultSet photosRs = stmt2.executeQuery(commonPhotosQuery)) {
                        // Iterate through the common photos and add to the matchPair
                        while (photosRs.next()) {
                            long photoId = photosRs.getLong("PHOTO_ID");
                            String photoLink = photosRs.getString("PHOTO_LINK");
                            long albumId = photosRs.getLong("ALBUM_ID");
                            String albumName = photosRs.getString("ALBUM_NAME");
                            PhotoInfo photoInfo = new PhotoInfo(photoId, albumId, photoLink, albumName);
                            matchPair.addSharedPhoto(photoInfo);
                        }
                    }
    
                    results.add(matchPair);
                    count++;
                }
            }
        } catch (SQLException e) {
            System.err.println("Error executing query: " + e.getMessage());
        }
    
        return results;
    }

    @Override
    // Query 6
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the IDs, first names, and last names of each of the two users in
    //            the top <num> pairs of users who are not friends but have a lot of
    //            common friends
    //        (B) For each pair identified in (A), find the IDs, first names, and last names
    //            of all the two users' common friends
    public FakebookArrayList<UsersPair> suggestFriends(int num) throws SQLException {
        FakebookArrayList<UsersPair> results = new FakebookArrayList<UsersPair>("\n");

        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly)) {
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                UserInfo u1 = new UserInfo(16, "The", "Hacker");
                UserInfo u2 = new UserInfo(80, "Dr.", "Marbles");
                UserInfo u3 = new UserInfo(192, "Digit", "Le Boid");
                UsersPair up = new UsersPair(u1, u2);
                up.addSharedFriend(u3);
                results.add(up);
            */
            
            // Create or replace a bidirectional friendship view to simplify querying mutual friends
            stmt.executeUpdate(
                "CREATE OR REPLACE VIEW BidirectionalFriends AS " +
                "SELECT USER1_ID AS USER_ID1, USER2_ID AS USER_ID2 FROM " + FriendsTable + " " +
                "UNION " +
                "SELECT USER2_ID AS USER_ID1, USER1_ID AS USER_ID2 FROM " + FriendsTable
            );

            // Find pairs of users who have mutual friends but are not friends themselves
            // stmt.executeUpdate( 
            //     "CREATE OR REPLACE VIEW mutualFriends AS " + 
            //     "SELECT BF1.USER_ID1 AS USER1_ID, BF2.USER_ID1 AS USER2_ID, BF1.USER_ID2 AS MF_ID, COUNT(*) AS MUTUAL_FRIENDS_COUNT " +
            //     "FROM BidirectionalFriends BF1, BidirectionalFriends BF2, " + FriendsTable + " F " + 
            //     "WHERE BF1.USER_ID1 != BF2.USER_ID1 AND BF1.USER_ID2 = BF2.USER_ID2 " +
            //     "AND ((BF1.USER_ID1 != F.USER1_ID AND BF2.USER_ID1 != F.USER2_ID) OR (BF1.USER_ID1 != F.USER2_ID AND BF2.USER_ID1 != F.USER1_ID)) " + 
            //     "AND BF1.USER_ID1 < BF2.USER_ID1 " +
            //     "GROUP BY BF1.USER_ID1, BF2.USER_ID1 " +
            //     "ORDER BY MUTUAL_FRIENDS_COUNT DESC, BF1.USER_ID1 ASC, BF2.USER_ID1 ASC "
            // );

            stmt.executeUpdate(
                "CREATE OR REPLACE VIEW mutualFriends AS " +
                "SELECT BF1.USER_ID1 AS USER1_ID, BF2.USER_ID1 AS USER2_ID, BF1.USER_ID2 AS MF_ID, COUNT(*) AS MUTUAL_FRIENDS_COUNT " +
                "FROM BidirectionalFriends BF1, BidirectionalFriends BF2 " +
                "WHERE BF1.USER_ID1 != BF2.USER_ID1 AND BF1.USER_ID2 = BF2.USER_ID2 " +
                "AND NOT EXISTS (SELECT 1 FROM " + FriendsTable + " F " +
                               "WHERE (F.USER1_ID = BF1.USER_ID1 AND F.USER2_ID = BF2.USER_ID1) " +
                               "OR (F.USER1_ID = BF2.USER_ID1 AND F.USER2_ID = BF1.USER_ID1)) " +
                "AND BF1.USER_ID1 < BF2.USER_ID1 " +
                "GROUP BY BF1.USER_ID1, BF2.USER_ID1, BF1.USER_ID2 " +
                "ORDER BY MUTUAL_FRIENDS_COUNT DESC, BF1.USER_ID1 ASC, BF2.USER_ID1 ASC "
            );


            ResultSet rst = stmt.executeQuery(
                "SELECT USER1_ID, USER2_ID, MF_ID, MUTUAL_FRIENDS_COUNT " +
                "FROM mutualFriends "
            );

            ArrayList<Long> user1List = new ArrayList<>();
            ArrayList<Long> user2List = new ArrayList<>();
            ArrayList<ArrayList<Long>> mutualFriendList = new ArrayList<>();

            rst.next();
            Long prev1 = rst.getLong("USER1_ID");
            Long prev2 = rst.getLong("USER2_ID");
            ArrayList<Long> curMutualFriends = new ArrayList<>();
            curMutualFriends.add(rst.getLong("MF_ID"));
            while (rst.next()) {

                Long cur1 = rst.getLong("USER1_ID");
                Long cur2 = rst.getLong("USER2_ID");

                if (!cur1.equals(prev1) && !cur2.equals(prev2)) {
                    user1List.add(cur1);
                    user2List.add(cur2);
                    mutualFriendList.add(curMutualFriends);
                } else {
                    curMutualFriends.add(rst.getLong("MF_ID"));

                }
                
                prev1 = cur1;
                prev2 = cur2;
            }

            rst.close();


            // Fetch mutual friends for each pair
            for (int i = 0; i < num; i++) {
                Long user1Id = user1List.get(i);
                Long user2Id = user2List.get(i);

                rst = stmt.executeQuery(
                    "SELECT FIRST_NAME, LAST_NAME " +
                    "FROM " + UsersTable + " U1 " + UsersTable + " U2 " +
                    "WHERE U1.USER_ID = " + user1Id + " AND U2.USER_ID = " + user2Id + " "
                );
                
                rst.next();
                UserInfo user1 = new UserInfo(user1Id, rst.getString("FIRST_NAME"), rst.getString("LAST_NAME"));
                UserInfo user2 = new UserInfo(user2Id, rst.getString("FIRST_NAME"), rst.getString("LAST_NAME"));
                UsersPair pair = new UsersPair(user1, user2);

                while (mutualFriendList.get(i).size() > 0) {
                    Long mutualFriendId = mutualFriendList.get(i).remove(0);
                    rst = stmt.executeQuery(
                        "SELECT FIRST_NAME, LAST_NAME " +
                        "FROM " + UsersTable + " " +
                        "WHERE USER_ID = " + mutualFriendId + " "
                    );
                    rst.next();
                    UserInfo mutualFriend = new UserInfo(mutualFriendId, rst.getString("FIRST_NAME"), rst.getString("LAST_NAME"));
                    pair.addSharedFriend(mutualFriend);
                }

                results.add(pair);
                rst.close();
            }

            stmt.executeUpdate("DROP VIEW BidirectionalFriends");
            stmt.executeUpdate("DROP VIEW mutualFriends");
            stmt.close();

        } catch (SQLException e) {
            System.err.println("Error executing query: " + e.getMessage());
        }

        return results;
    }

    @Override
    // Query 7
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the name of the state or states in which the most events are held
    //        (B) Find the number of events held in the states identified in (A)
    public EventStateInfo findEventStates() throws SQLException {
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll,
                FakebookOracleConstants.ReadOnly)) {
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                EventStateInfo info = new EventStateInfo(50);
                info.addState("Kentucky");
                info.addState("Hawaii");
                info.addState("New Hampshire");
                return info;
            */
            stmt.executeUpdate(
            "CREATE VIEW EventCounts AS " +
            "SELECT C.STATE_NAME, COUNT(*) AS EVENT_COUNT " +
            "FROM" + EventsTable + " E " +
            "JOIN" + CitiesTable + " C ON E.EVENT_CITY_ID = C.CITY_ID " +
            "GROUP BY C.STATE_NAME"
            );

            // Find the maximum event count
            ResultSet rst = stmt.executeQuery(
                "SELECT MAX(EVENT_COUNT) AS MAX_COUNT FROM EventCounts"
            );

            int maxEventCount = 0;
            if (rst.next()) {
                maxEventCount = rst.getInt(1);
            }
            rst.close();

            // Retrieve states with the maximum event count
            ResultSet rs = stmt.executeQuery(
                "SELECT STATE_NAME, EVENT_COUNT FROM EventCounts " +
                "WHERE EVENT_COUNT = " + maxEventCount + " " +
                "ORDER BY STATE_NAME ASC"
            );

            EventStateInfo eventStateInfo = new EventStateInfo(maxEventCount);

            while (rs.next()) {
                eventStateInfo.addState(rs.getString(1));
            }
            rs.close();

            stmt.executeUpdate("DROP VIEW EventCounts");

            return eventStateInfo;
        } catch (SQLException e) {
            System.err.println(e.getMessage());
            return new EventStateInfo(-1);
        }
    }

    @Override
    // Query 8
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the ID, first name, and last name of the oldest friend of the user
    //            with User ID <userID>
    //        (B) Find the ID, first name, and last name of the youngest friend of the user
    //            with User ID <userID>
    public AgeInfo findAgeInfo(long userID) throws SQLException {
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll,
                FakebookOracleConstants.ReadOnly)) {
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                UserInfo old = new UserInfo(12000000, "Galileo", "Galilei");
                UserInfo young = new UserInfo(80000000, "Neil", "deGrasse Tyson");
                return new AgeInfo(old, young);
            */
            return new AgeInfo(new UserInfo(-1, "UNWRITTEN", "UNWRITTEN"), new UserInfo(-1, "UNWRITTEN", "UNWRITTEN")); // placeholder for compilation
        } catch (SQLException e) {
            System.err.println(e.getMessage());
            return new AgeInfo(new UserInfo(-1, "ERROR", "ERROR"), new UserInfo(-1, "ERROR", "ERROR"));
        }
    }

    @Override
    // Query 9
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find all pairs of users that meet each of the following criteria
    //              (i) same last name
    //              (ii) same hometown
    //              (iii) are friends
    //              (iv) less than 10 birth years apart
    public FakebookArrayList<SiblingInfo> findPotentialSiblings() throws SQLException {
        FakebookArrayList<SiblingInfo> results = new FakebookArrayList<SiblingInfo>("\n");

        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll,
                FakebookOracleConstants.ReadOnly)) {
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                UserInfo u1 = new UserInfo(81023, "Kim", "Kardashian");
                UserInfo u2 = new UserInfo(17231, "Kourtney", "Kardashian");
                SiblingInfo si = new SiblingInfo(u1, u2);
                results.add(si);
            */
        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }

        return results;
    }

    // Member Variables
    private Connection oracle;
    private final String UsersTable = FakebookOracleConstants.UsersTable;
    private final String CitiesTable = FakebookOracleConstants.CitiesTable;
    private final String FriendsTable = FakebookOracleConstants.FriendsTable;
    private final String CurrentCitiesTable = FakebookOracleConstants.CurrentCitiesTable;
    private final String HometownCitiesTable = FakebookOracleConstants.HometownCitiesTable;
    private final String ProgramsTable = FakebookOracleConstants.ProgramsTable;
    private final String EducationTable = FakebookOracleConstants.EducationTable;
    private final String EventsTable = FakebookOracleConstants.EventsTable;
    private final String AlbumsTable = FakebookOracleConstants.AlbumsTable;
    private final String PhotosTable = FakebookOracleConstants.PhotosTable;
    private final String TagsTable = FakebookOracleConstants.TagsTable;
}
