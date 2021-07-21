package com.vamsi.Threads;

import com.vamsi.Threads.Model.Artists;
import com.vamsi.Threads.Model.DataSources;
import com.vamsi.Threads.Model.SongArtist;

import java.util.List;

public class Main {

    public static void main(String[] args) {
        DataSources dataSources = new DataSources();
        if (!dataSources.open()){
            System.out.println("cannot connect");
            return;

        }
//        List<Artists> artists = dataSources.queryArtist(5);
//        if(artists == null){
//            System.out.println("No artists");
//            return;
//        }
//        for (Artists artists1: artists){
//            System.out.println("ID = "+artists1.getId()+" name "+artists1.getName());
//        }
//        List<SongArtist> songArtists = dataSources.QuerySongsArtist("Go Your Own Way",DataSources.ORDER_BY_ASC);
//        if (songArtists==null){
//            System.out.println("Not updated");
//            return;
//        }
//        for (SongArtist songArtist: songArtists){
//            System.out.println("Artist Name: "+ songArtist.getArtistName()+"\n"+"Album Name: "+songArtist.getAlbumName()+
//                    "\n"+"ID: "+songArtist.getTrack());
//            System.out.println("----------------------------------------------------------");
//        }
        System.out.println(dataSources.getCount(DataSources.TABLE_SONGS));
        System.out.println(dataSources.createView());
        List<SongArtist> songArtists = dataSources.queryView("Go Your Own Way");
        if (songArtists==null){
            System.out.println("Data not updated");
            return;
        }
        for (SongArtist songArtist: songArtists){
            System.out.println("Artist Name: "+ songArtist.getArtistName()+"\n"+"Album Name: "+songArtist.getAlbumName()+
                    "\n"+"ID: "+songArtist.getTrack());
            System.out.println("----------------------------------------------------------");
        }
        dataSources.insertSong("Touch of Grey",1,"In The Dark","Grateful Dead");
        dataSources.close();
    }
}
