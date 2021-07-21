package com.vamsi.Threads.Model;

import java.net.PortUnreachableException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class DataSources {
    public static final String DB_NAME = "music.db";
    public static final String CONNECTION_STRING = "jdbc:sqlite:C:\\Music\\NewDataBase\\"+DB_NAME;
    public static final String TABLE_ALBUMS = "albums";
    public static final String COLUMN_ALBUM_ID = "_id";
    public static final String COLUMN_ALBUM_NAME = "name";
    public static final String COLUMN_ALBUM_ARTIST = "artist";
    public static final int INDEX_ALBUM_ID =1;
    public static final int INDEX_ALBUM_NAME =2;
    public static final int INDEX_ALBUM_ARTIST =3;
    public static final String TABLE_ARTISTS = "artists";
    public static final String COLUMN_ARTISTS_ID ="_id";
    public static final String COLUMN_ARTISTS_NAME = "name";
    public static final int INDEX_ARTISTS_ID =1;
    public static final int INDEX_ARTISTS_NAME =2;

    public static final String TABLE_SONGS = "songs";
    public static final String COLUMN_SONG_ID = "_id";
    public static final String COLUMN_SONG_TRACK = "track";
    public static final String COLUMN_SONG_TITLE = "title";
    public static final String COLUMN_SONG_ALBUM = "album";
    public static final int INDEX_COLUMN_ID =1;
    public static final int INDEX_COLUMN_TRACK =2;
    public static final int INDEX_COLUMN_TITLE =3;
    public static final int INDEX_COLUMN_ALBUM =4;

    public static final int ORDER_BY_NONE =1;
    public static final int ORDER_BY_ASC =2;
    public static final int ORDER_BY_DESC = 3;

    public static final String QUERY_ALBUMS_SONGS_ARTIST = "SELECT "+ TABLE_ARTISTS+"."+COLUMN_ARTISTS_NAME+", "
            +TABLE_ALBUMS+"."+COLUMN_ALBUM_NAME+", "+TABLE_SONGS+"."+COLUMN_SONG_TRACK+" FROM "+TABLE_SONGS+
            " INNER JOIN "+ TABLE_ALBUMS+ " ON "+ TABLE_SONGS+"."+COLUMN_SONG_ALBUM+" = "+TABLE_ALBUMS+"."
            +COLUMN_ALBUM_ID+" INNER JOIN "+TABLE_ARTISTS+ " ON "+ TABLE_ALBUMS+"."+COLUMN_ALBUM_ARTIST
            +" = "+TABLE_ARTISTS+"."+COLUMN_ARTISTS_ID+" WHERE "+TABLE_SONGS+"."+COLUMN_SONG_TITLE+" = \"";
    public static final String QUERY_ALBUMS_SONGS_ARTIST1 = " ORDER BY "+ TABLE_ARTISTS+"."+COLUMN_ARTISTS_NAME
            +", "+TABLE_ALBUMS+"."+COLUMN_ALBUM_NAME+" COLLATE NOCASE ";

    public static final String TABLE_ARTIST_SONG_VIEW = "artist_list";

    public static final String CREATE_ARTIST_FOR_SONG_VIEW = "CREATE VIEW IF NOT EXISTS " +
            TABLE_ARTIST_SONG_VIEW + " AS SELECT " + TABLE_ARTISTS + "." + COLUMN_ARTISTS_NAME + ", " +
            TABLE_ALBUMS + "." + COLUMN_ALBUM_NAME + " AS " + COLUMN_SONG_ALBUM + ", " +
            TABLE_SONGS + "." + COLUMN_SONG_TRACK + ", " + TABLE_SONGS + "." + COLUMN_SONG_TITLE +
            " FROM " + TABLE_SONGS +
            " INNER JOIN " + TABLE_ALBUMS + " ON " + TABLE_SONGS +
            "." + COLUMN_SONG_ALBUM + " = " + TABLE_ALBUMS + "." + COLUMN_ALBUM_ID +
            " INNER JOIN " + TABLE_ARTISTS + " ON " + TABLE_ALBUMS + "." + COLUMN_ALBUM_ARTIST +
            " = " + TABLE_ARTISTS + "." + COLUMN_ARTISTS_ID +
            " ORDER BY " +
            TABLE_ARTISTS + "." + COLUMN_ARTISTS_NAME + ", " +
            TABLE_ALBUMS + "." + COLUMN_ALBUM_NAME + ", " +
            TABLE_SONGS + "." + COLUMN_SONG_TRACK;
    public static final String QUERY_VIEW_SONG_INFO = "SELECT "+ COLUMN_ARTISTS_NAME+", "+COLUMN_SONG_ALBUM+", "
            +COLUMN_SONG_TRACK+" FROM "+ TABLE_ARTIST_SONG_VIEW+ " WHERE "+ COLUMN_SONG_TITLE+"= \"";
    public static final String QUERY_VIEW_SONG_STATEMENT = "SELECT "+ COLUMN_ARTISTS_NAME+", "+COLUMN_SONG_ALBUM+", "
            +COLUMN_SONG_TRACK+" FROM "+ TABLE_ARTIST_SONG_VIEW+ " WHERE "+ COLUMN_SONG_TITLE+"= ?";

    public static final String INSERT_ARTISTS ="INSERT INTO "+TABLE_ARTISTS+'('+COLUMN_ARTISTS_NAME+") VALUES(?)";
    public static final String INSERT_ALBUMS ="INSERT INTO "+TABLE_ALBUMS+'('+COLUMN_ALBUM_NAME+", "+COLUMN_ALBUM_ARTIST+") VALUES(?,?)";
    public static final String INSERT_SONG ="INSERT INTO "+TABLE_SONGS+'('+COLUMN_SONG_TRACK+", "+COLUMN_SONG_TITLE+", "+COLUMN_SONG_ALBUM+") VALUES(?,?,?)";
    public static final String QUERY_ARTIST = "SELECT "+COLUMN_ARTISTS_ID+" FROM "+TABLE_ARTISTS+" WHERE "+COLUMN_ARTISTS_NAME
            +" =? ";
    public static final String QUERY_ALBUM = "SELECT "+COLUMN_ALBUM_ID+" FROM "+TABLE_ALBUMS+" WHERE "+COLUMN_ALBUM_NAME
            +" =?";
    private PreparedStatement queryViewStatement;
    private PreparedStatement insertIntoArtists;
    private PreparedStatement insertIntoAlbums;
    private PreparedStatement insertIntoSongs;
    private PreparedStatement queryArtist;
    private PreparedStatement queryAlbum;

    private Connection conn;
    public boolean open(){
        try {
            conn= DriverManager.getConnection(CONNECTION_STRING);
            queryViewStatement=conn.prepareStatement(QUERY_VIEW_SONG_STATEMENT);
            insertIntoAlbums=conn.prepareStatement(INSERT_ALBUMS,Statement.RETURN_GENERATED_KEYS);
            insertIntoArtists=conn.prepareStatement(INSERT_ARTISTS,Statement.RETURN_GENERATED_KEYS);
            insertIntoSongs=conn.prepareStatement(INSERT_SONG);
            queryArtist= conn.prepareStatement(QUERY_ARTIST);
            queryAlbum = conn.prepareStatement(QUERY_ALBUM);
            return true;
        }catch (SQLException e){
            e.printStackTrace();
            return false;
        }
    }
    public void close(){
        try {
            if (queryArtist!=null){queryArtist.close();}
            if (queryAlbum!=null){queryAlbum.close();}
            if (queryViewStatement!=null){queryViewStatement.close();}
            if (insertIntoSongs!=null){insertIntoSongs.close();}
            if (insertIntoArtists!=null){insertIntoArtists.close();}
            if (insertIntoAlbums!=null){insertIntoAlbums.close();}
            if (conn!=null){conn.close();}
        }catch (SQLException e){
            e.printStackTrace();
        }
    }

    public List<Artists> queryArtist(int sortNumber){
        StringBuilder stringBuilder = new StringBuilder("SELECT * FROM ");
        stringBuilder.append(TABLE_ARTISTS);
        if (sortNumber!=ORDER_BY_NONE){
            stringBuilder.append(" ORDER BY ");
            stringBuilder.append(COLUMN_ARTISTS_NAME);
            stringBuilder.append(" COLLATE NOCASE ");
            if (sortNumber==ORDER_BY_ASC){
                stringBuilder.append("ASC");
            }else {stringBuilder.append("DESC");}
        }
        try(Statement statement = conn.createStatement();
            ResultSet resultSet = statement.executeQuery(stringBuilder.toString())) {
//            statement = conn.createStatement();
//            resultSet = statement.executeQuery("SELECT * FROM "+TABLE_ARTISTS);
            List<Artists> artists = new ArrayList<>();
            while (resultSet.next()){
                Artists artist = new Artists();
                artist.setId(resultSet.getInt(INDEX_ARTISTS_ID));
                artist.setName(resultSet.getString(INDEX_ARTISTS_NAME));
                artists.add(artist);
            }
            return artists;
        }catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
//        }finally {
//            try {
//                if (resultSet!=null){
//                    resultSet.close();
//                }
//            }catch (SQLException e){
//                e.printStackTrace();
//            }
//            try {
//                if (statement!=null){
//                    statement.close();
//                }
//            }catch (SQLException e){
//                e.printStackTrace();
//            }
//        }
    }
    public List<String> queryAlbumsAndArtists(String name, int sortNumber){
        StringBuilder stringBuilder = new StringBuilder("SELECT ");
        stringBuilder.append(TABLE_ALBUMS);
        stringBuilder.append(".");
        stringBuilder.append(COLUMN_ALBUM_NAME);
        stringBuilder.append(" FROM ");
        stringBuilder.append(TABLE_ALBUMS);
        stringBuilder.append(" INNER JOIN ");
        stringBuilder.append(TABLE_ARTISTS);
        stringBuilder.append(" ON ");
        stringBuilder.append(TABLE_ALBUMS);
        stringBuilder.append(".");
        stringBuilder.append(COLUMN_ALBUM_ARTIST);
        stringBuilder.append(" = ");
        stringBuilder.append(TABLE_ARTISTS);
        stringBuilder.append(".");
        stringBuilder.append(COLUMN_ARTISTS_ID);
        stringBuilder.append(" WHERE ");
        stringBuilder.append(TABLE_ARTISTS+"."+COLUMN_ARTISTS_NAME);
        stringBuilder.append(" = \"");
        stringBuilder.append(name);
        stringBuilder.append("\"");
        if (sortNumber!= ORDER_BY_NONE){
            stringBuilder.append(" ORDER BY ");
            stringBuilder.append(TABLE_ALBUMS+"."+COLUMN_ALBUM_NAME);
            if (sortNumber==ORDER_BY_ASC){
                stringBuilder.append(" ASC");
            }else {
                stringBuilder.append(" DESC");
            }
        }
        try(Statement statement = conn.createStatement();
            ResultSet resultSet = statement.executeQuery(stringBuilder.toString())) {
            List<String> AlbumAndArtist = new ArrayList<>();
            while (resultSet.next()){
                AlbumAndArtist.add(resultSet.getString(1));
            }
            return AlbumAndArtist;
        }catch (SQLException e){
            e.printStackTrace();
            return null;
        }

    }
    public List<SongArtist> QuerySongsArtist(String name, int sortedNumber){
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(QUERY_ALBUMS_SONGS_ARTIST);
        stringBuilder.append(name);
        stringBuilder.append("\"");
        if (sortedNumber!=ORDER_BY_NONE){
            stringBuilder.append(QUERY_ALBUMS_SONGS_ARTIST1);
            if (sortedNumber==ORDER_BY_ASC){
                stringBuilder.append("ASC");
            }else {
                stringBuilder.append("DESC");
            }
        }
        try(Statement statement = conn.createStatement();
            ResultSet resultSet = statement.executeQuery(stringBuilder.toString())) {
            List<SongArtist> res = new ArrayList<>();
            while (resultSet.next()){
                SongArtist songArtist = new SongArtist();
                songArtist.setAlbumName(resultSet.getString(2));
                songArtist.setArtistName(resultSet.getString(1));
                songArtist.setTrack(resultSet.getInt(3));
                res.add(songArtist);
            }
            return res;
        }catch (SQLException e){
            e.printStackTrace();
            return null;
        }
    }
    public void querySongsMetadata(){
        String comm = "SELECT * FROM "+TABLE_SONGS;
        try(Statement statement = conn.createStatement();
            ResultSet resultSet = statement.executeQuery(comm)){
            ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
            int len = resultSetMetaData.getColumnCount();
            for (int i=0;i<len;i++){
                System.out.format("Column %d in the songs table is names %s\n",i,resultSetMetaData.getColumnName(i));
            }
        }catch (SQLException e){
            e.printStackTrace();
        }

    }
    public int getCount(String table){
        String comm = "SELECT COUNT(*) AS count, MIN(_id) AS min FROM "+table;
        try(Statement statement = conn.createStatement();
            ResultSet resultSet = statement.executeQuery(comm)){
            int count = resultSet.getInt("count");
            int min = resultSet.getInt("min");
            System.out.format("Count = %d, Min = %d\n",count,min);
            return count;
        }catch (SQLException e){
            e.printStackTrace();
            return -1;

        }

    }
    public boolean createView(){
        try(Statement statement = conn.createStatement()){
            statement.execute(CREATE_ARTIST_FOR_SONG_VIEW);
            return true;
        }catch (SQLException e){
            e.printStackTrace();
            return false;
        }
    }
    public List<SongArtist> queryView(String title){
        try {
            queryViewStatement.setString(1,title);
            ResultSet resultSet = queryViewStatement.executeQuery();

            List<SongArtist> songArtists = new ArrayList<>();
            while (resultSet.next()) {
                SongArtist songArtist = new SongArtist();
                songArtist.setArtistName(resultSet.getString(1));
                songArtist.setAlbumName(resultSet.getString(2));
                songArtist.setTrack(resultSet.getInt(3));
                songArtists.add(songArtist);
            }
            return songArtists;
        }catch (SQLException e){
            e.printStackTrace();
            return null;
        }

    }
    private int insertArtist(String name) throws SQLException{
        queryArtist.setString(1,name);
        ResultSet resultSet = queryArtist.executeQuery();
        if (resultSet.next()){
            return resultSet.getInt(1);
        }else {
            insertIntoArtists.setString(1,name);
            int affectedRows = insertIntoArtists.executeUpdate();
            if (affectedRows!=1){
                throw new SQLException("There is error in uploading");
            }
            ResultSet generatedKey = insertIntoArtists.getGeneratedKeys();
            if (generatedKey.next()){
                return generatedKey.getInt(1);
            }else {
                throw new SQLException("Key was not generated");
            }
        }
    }
    private int insertAlbum(String name,int artistId) throws SQLException{
        queryAlbum.setString(1,name);
        ResultSet resultSet = queryAlbum.executeQuery();
        if (resultSet.next()){
            return resultSet.getInt(1);
        }else {
            insertIntoAlbums.setString(1,name);
            insertIntoAlbums.setInt(2,artistId);
            int affectedRows = insertIntoAlbums.executeUpdate();
            if (affectedRows!=1){
                throw new SQLException("There is error in uploading");
            }
            ResultSet generatedKey = insertIntoAlbums.getGeneratedKeys();
            if (generatedKey.next()){
                return generatedKey.getInt(1);
            }else {
                throw new SQLException("Key was not generated");
            }
        }
    }
    public void insertSong(String artist,int track,String title,String album) {
        try{
            conn.setAutoCommit(false);
            int artistNum = insertArtist(artist);
            int albumNum = insertAlbum(album,artistNum);
            insertIntoSongs.setInt(1,track);
            insertIntoSongs.setString(2,title);
            insertIntoSongs.setInt(3,albumNum);
            int affectedRows = insertIntoSongs.executeUpdate();

            if (affectedRows==1){
                conn.commit();
            }else {
                throw  new SQLException("The song inserted fail");
            }
        }catch (Exception e){
            e.printStackTrace();
            try{
                System.out.println("Performing Rollback");
                conn.rollback();
            }catch (SQLException d){
                d.printStackTrace();
            }
        }finally {
            try{
                System.out.println("Setting default commit behaviour");
                conn.setAutoCommit(true);
            }catch (SQLException a){
                a.printStackTrace();
            }
        }
//
    }
}
