import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

public class UniqueIPAddressCounter {
    private static final String TEMP_DIR = "tempChunks";

    public static void main(String[] args) {
        String filename = "src/main/resources/ips.txt";
        try {
            long uniqueCount = countUniqueIPAddresses(filename);
            System.out.println("Number of unique IP addresses: " + uniqueCount);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static long countUniqueIPAddresses(String filename) throws IOException {
        // split the file into chunks and store unique addresses in temporary files
        createTempDir();
        List<File> tempFiles = splitIntoChunks(filename);

        // merge sorted unique addresses from temporary files
        long uniqueCount = mergeAndCountUnique(tempFiles);

        // cleanup temporary files
        cleanupTempFiles(tempFiles);

        return uniqueCount;
    }

    private static void createTempDir() throws IOException {
        Files.createDirectories(Paths.get(TEMP_DIR));
    }

    private static List<File> splitIntoChunks(String filename) throws IOException {
        List<File> tempFiles = new ArrayList<>();
        Set<String> ipSet = new HashSet<>();
        int chunkSize = 100000; // number of lines per chunk
        int lineCount = 0;

        try (BufferedReader br = new BufferedReader(new FileReader(filename))) {
            String line;
            while ((line = br.readLine()) != null) {
                ipSet.add(line.trim());
                lineCount++;

                if (lineCount >= chunkSize) {
                    tempFiles.add(writeChunkToFile(ipSet));
                    ipSet.clear();
                    lineCount = 0;
                }
            }

            // write remaining IPs to a final chunk
            if (!ipSet.isEmpty()) {
                tempFiles.add(writeChunkToFile(ipSet));
            }
        }

        return tempFiles;
    }

    private static File writeChunkToFile(Set<String> ipSet) throws IOException {
        File tempFile = new File(TEMP_DIR, "chunk_" + System.currentTimeMillis() + ".txt");
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(tempFile))) {
            for (String ip : ipSet) {
                bw.write(ip);
                bw.newLine();
            }
        }
        return tempFile;
    }

    private static long mergeAndCountUnique(List<File> tempFiles) throws IOException {
        Set<String> uniqueIPs = new HashSet<>();
        List<BufferedReader> readers = new ArrayList<>();

        for (File file : tempFiles) {
            readers.add(new BufferedReader(new FileReader(file)));
        }

        PriorityQueue<String> minHeap = new PriorityQueue<>();

        for (BufferedReader reader : readers) {
            String line = reader.readLine();
            if (line != null) {
                minHeap.offer(line);
            }
        }

        while (!minHeap.isEmpty()) {
            String ip = minHeap.poll();
            uniqueIPs.add(ip);

            for (BufferedReader reader : readers) {
                if (reader.ready() && ip.equals(reader.readLine())) {
                    String nextLine = reader.readLine();
                    if (nextLine != null) {
                        minHeap.offer(nextLine);
                    }
                    break;
                }
            }
        }

        for (BufferedReader reader : readers) {
            reader.close();
        }

        return uniqueIPs.size();
    }

    private static void cleanupTempFiles(List<File> tempFiles) {
        for (File file : tempFiles) {
            file.delete();
        }
        new File(TEMP_DIR).delete();
    }
}
