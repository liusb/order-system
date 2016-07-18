package com.alibaba.middleware.race.store;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class FilePath {

    public static boolean exists(String file) {
        return Files.exists(Paths.get(file));
    }

    public static boolean createFile(String file) {
        try {
            Files.createFile(Paths.get(file));
            return true;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    public static boolean deleteFile(String file) {
        try {
            return Files.deleteIfExists(Paths.get(file));
        } catch(IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    public static boolean createDir(String path) {
        try {
            Files.createDirectories(Paths.get(path));
            return true;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    public static boolean deleteDir(String path) {
        try {
            Files.delete(Paths.get(path));
            return true;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    public static long fileSize(String file) {
        try {
            return Files.size(Paths.get(file));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return -1;
    }

    public static void main(String[] args) {

        if(exists("./store")) {
            System.out.print("exist function error!!");
        }
        if(!createDir("./store")) {
            System.out.print("createDir function error!!");
        }
        if(!createFile("./store/good.db")) {
            System.out.print("createFile function error!!");
        }
        if(fileSize("./store/good.db") == -1) {
            System.out.print("file size function error!!");
        }else {
            System.out.print("file size is :" + fileSize("./store"));
        }
        if(!deleteFile("./store/good.db")) {
            System.out.print("deleteFile function error!!");
        }
        if(!deleteDir("./store")) {
            System.out.print("deleteDir function error!!");
        }

    }
    
}
