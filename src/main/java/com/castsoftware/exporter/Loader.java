/*
 *  Friendly exporter for Neo4j - Copyright (C) 2020  Hugo JOBY
 *
 *      This library is free software; you can redistribute it and/or modify it under the terms
 *      of the GNU Lesser General Public License as published by the Free Software Foundation;
 *      either version 2.1 of the License, or (at your option) any later version.
 *      This library is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 *      without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 *      See the GNU Lesser General Public License for more details.
 *
 *      You should have received a copy of the GNU Lesser General Public License along with this library;
 *      If not, see <https://www.gnu.org/licenses/>.
 */


package com.castsoftware.exporter;

import com.castsoftware.exceptions.ProcedureException;
import com.castsoftware.exceptions.file.FileCorruptedException;
import com.castsoftware.results.OutputMessage;
import org.neo4j.graphdb.*;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.io.*;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

public class Loader {

    // Return message queue
    private static final List<OutputMessage> MESSAGE_QUEUE = new ArrayList<>();

    // Default values ( Should be added to a property file )
    private static final String DELIMITER = IOProperties.Property.CSV_DELIMITER.toString();
    private static final String EXTENSION = IOProperties.Property.CSV_EXTENSION.toString();
    private static final String INDEX_COL = IOProperties.Property.INDEX_COL.toString();
    private static final String INDEX_OUTGOING = IOProperties.Property.INDEX_OUTGOING.toString();
    private static final String INDEX_INCOMING = IOProperties.Property.INDEX_INCOMING.toString();
    private static final String RELATIONSHIP_PREFIX = IOProperties.Property.PREFIX_RELATIONSHIP_FILE.toString();
    private static final String NODE_PREFIX = IOProperties.Property.PREFIX_NODE_FILE.toString();

    // Static Members
    private static Long countLabelCreated;
    private static Long countRelationTypeCreated;
    private static Long ignoredFile;
    private static Long nodeCreated;
    private static Long relationshipCreated;

    // Binding map between csv ID and Neo4j created nodes. Only the Node id is stored here, to limit the usage of heap memory.
    private static Map<Long, Long> idBindingMap;

    @Context
    public GraphDatabaseService db;

    @Context
    public Log log;

    /**
     * Convert a String containing a Neo4j Type to a Java Type
     * Handled type are : Boolean, Char, Byte, Short, Long, Double, LocalDate, OffsetTime, LocalTime, ZoneDateTime.
     * If none of these types are detected, it will return the value as a String.
     * <u>Warning :</u> TemporalAmount and org.neo4j.graphdb.spatial.Point are not detected
     * Check https://neo4j.com/docs/java-reference/current/java-embedded/property-values/index.html for more informations
     * <u>Warning :</u> The goal of this function is to reassign the correct Java type to the value discovered in the CSV. It mays detect the wrong type.
     *
     * @param value Neo4j Value as a string
     * @return Object of the Java Type associated to the discovered type within the string provided
     */
    private Object getNeo4jType(String value) {

        // Boolean
        try { return Integer.parseInt(value); } catch (NumberFormatException ignored) { }
        // Byte
        try { return Byte.parseByte(value); } catch (NumberFormatException ignored) { }
        // Short
        try { return Short.parseShort(value); } catch (NumberFormatException ignored) { }
        // Long
        try { return Long.parseLong(value); } catch (NumberFormatException ignored) { }
        // Double
        try { return Double.parseDouble(value); } catch (NumberFormatException ignored) { }

        // DateTimeFormatter covering all Neo4J Date Format  (cf : https://neo4j.com/docs/cypher-manual/current/syntax/temporal/ )
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("[YYYY-MM-DD]" +
                "[YYYYMMDD]" +
                "[YYYY-MM]" +
                "[YYYYMM]" +
                "[YYYY-Www-D]" +
                "[YYYY- W ww]" +
                "[YYYY W ww]" +
                "[YYYY- Q q-DD]" +
                "[YYYY Q q]" +
                "[YYYY-DDD]" +
                "[YYYYDDD]" +
                "[YYYY]");

        // LocalDate
        try { return LocalDate.parse(value, formatter); } catch (DateTimeParseException ignored) { }
        // OffsetTime
        try { return OffsetTime.parse(value, formatter); } catch (DateTimeParseException ignored) { }
        // LocalTime
        try { return LocalTime.parse(value, formatter); } catch (DateTimeParseException ignored) { }
        // ZoneDateTime
        try { return ZonedDateTime.parse(value, formatter); } catch (DateTimeParseException ignored) { }

        // Char
        if (value.length() == 1) return value.charAt(0);

        // String
        return value;
    }

    /**
     * Remove EOL token and split the row to List<String>
     * @param input CVS row to sanitize
     * @return Sanitized string
     */
    private List<String> sanitizeCSVInput(String input) {
        return Arrays.asList(input.replaceAll("\\\\r\\\\n", "").split(DELIMITER));
    }

    /**
     * Get the label stored within the filename by removing the prefix and the extension
     * @param filename
     * @return
     */
    private String getLabelFromFilename(String filename) {
        return filename.replace(RELATIONSHIP_PREFIX, "")
                .replace(NODE_PREFIX, "")
                .replace(EXTENSION, "");
    }

    /**
     * Create a Node based on provided header and values.
     * If a value is empty, it will not be added as a property to the node.
     * To make the com.castsoftware.exporter more generic, the conversion from CSV values to Java Values does not necessitate POJOs object.
     * However, the drawback is that this conversion can create some errors. @see Loader.getNeo4jType() for more information.
     * @param label Label that will be give to the node
     * @param headers Headers as a list of String
     * @param values Values as a list of String
     */
    private void createNode(Label label, List<String> headers, List<String> values) {
        int indexCol = headers.indexOf(INDEX_COL);
        Long id = Long.parseLong(values.get(indexCol));

        Node n = db.createNode(label);

        int minSize = Math.min(values.size(), headers.size());
        for (int i = 0; i < minSize; i++) {
            if (i == indexCol || values.get(i).isEmpty()) continue; // Index col or empty value
            Object extractedVal = getNeo4jType(values.get(i));
            n.setProperty(headers.get(i), extractedVal);
        }

        nodeCreated ++;
        idBindingMap.put(id, n.getId()); // We need to keep a track of the csv id to bind node together later
    }

    /**
     * Create a relationship between two node. Source node ID and Destination node must be specified in the header and the value list.
     * If one of these information is missing the relationship will be ignored.
     * @param relationshipType The name of the relationship
     * @param headers List containing the value of the header
     * @param values List containing the value of the relationship
     */
    private void createRelationship(RelationshipType relationshipType, List<String> headers, List<String> values) {
        int indexOutgoing = headers.indexOf(INDEX_OUTGOING);
        Long idOutgoing = Long.parseLong(values.get(indexOutgoing));

        int indexIncoming = headers.indexOf(INDEX_INCOMING);
        Long idIncoming = Long.parseLong(values.get(indexIncoming));

        Long srcNodeId = idBindingMap.get(idOutgoing);
        Long destNodeId = idBindingMap.get(idIncoming);

        if( srcNodeId == null || destNodeId == null) return; // Ignore this relationship, at least one node is missing

        Node srcNode = db.getNodeById(srcNodeId);
        Node destNode = db.getNodeById(destNodeId);

        Relationship rel = srcNode.createRelationshipTo(destNode, relationshipType);

        int minSize = Math.min(values.size(), headers.size());
        for (int i = 0; i < minSize; i++) {
            if (i == indexOutgoing || i == indexIncoming || values.get(i).isEmpty()) continue; // Index col or empty value
            Object extractedVal = getNeo4jType(values.get(i));
            rel.setProperty(headers.get(i), extractedVal);
        }

        relationshipCreated++;

    }

    /**
     * Treat a node buffer by extracting the first row as a list of header value. Treat all the other rows as list of node's values.
     * @param associatedLabel Name of the label
     * @param nodeFileBuf BufferReader pointing to the node file
     * @throws IOException thrown if the procedure fails to read the buffer
     * @throws FileCorruptedException thrown if the file isn't in a good format ( If the headers are missing, or if it does not contains any Index Column)
     */
    private void treatNodeBuffer(String associatedLabel, BufferedReader nodeFileBuf) throws IOException, FileCorruptedException {
        String line;
        String headers = nodeFileBuf.readLine();
        if (headers == null) throw new FileCorruptedException("No header found in file.", "LOADxTNBU01");

        Label label = Label.label(associatedLabel);

        //Process header line
        List<String> headerList = sanitizeCSVInput(headers);
        if (!headerList.contains(INDEX_COL))
            throw new FileCorruptedException("No index column found in file.", "LOADxTNBU02");

        while ((line = nodeFileBuf.readLine()) != null) {
            List<String> values = sanitizeCSVInput(line);
            try {
                createNode(label, headerList, values);
            } catch (Exception e) {
                log.error("An error occurred during creation of node with label : ".concat(associatedLabel).concat(" and values : ").concat(String.join(DELIMITER, values)), e);
            }
        }
    }

    /**
     * Treat a relationship buffer by extracting the first row as a list of header value. Treat all the other rows as list of relationship's values.
     * @param associatedRelation Name of the relationship
     * @param relFileBuf BufferReader pointing to the relationship file
     * @throws IOException thrown if the procedure fails to read the buffer
     * @throws FileCorruptedException thrown if the file isn't in a good format ( If the headers are missing, or if it does not contains any Source or Destination index column)
     */
    private void treatRelBuffer(String associatedRelation, BufferedReader relFileBuf) throws IOException, FileCorruptedException {
        String line;
        String headers = relFileBuf.readLine();
        if (headers == null) throw new FileCorruptedException("No header found in file.", "LOADxTNBU01");

        RelationshipType relName = RelationshipType.withName(associatedRelation);

        List<String> headerList = sanitizeCSVInput(headers);
        if (!headerList.contains(INDEX_OUTGOING) || !headerList.contains(INDEX_INCOMING))
            throw new FileCorruptedException("Corrupted header (missing source or destination columns).", "LOADxTNBU02");

        while ((line = relFileBuf.readLine()) != null) {
            List<String> values = sanitizeCSVInput(line);
            createRelationship(relName, headerList, values);
        }

    }

    /**
     * Parse all files within zip file. For each file a BufferedReader will be open and stored as a Node BufferReader or a Relationship BufferReader.
     * The procedure will use the prefix in the filename to decide if it must be treated as a file containing node or relationships.
     * @param file The Zip file to be treated
     * @throws IOException
     */
    private void parseZip(File file) throws IOException {
        Map<BufferedReader, String> nodeBuffers = new HashMap<>();
        Map<BufferedReader, String> relBuffers = new HashMap<>();

        try (ZipFile zf = new ZipFile(file)) {
            Enumeration entries = zf.entries();

            while (entries.hasMoreElements()) {
                ZipEntry ze = (ZipEntry) entries.nextElement();
                String filename = ze.getName();
                if (ze.getSize() < 0) continue; // Empty entry

                try {
                    BufferedReader br = new BufferedReader(new InputStreamReader(zf.getInputStream(ze)));

                    if (filename.startsWith(RELATIONSHIP_PREFIX)) {
                        relBuffers.put(br, filename);
                    } else if (filename.startsWith(NODE_PREFIX)) {
                        nodeBuffers.put(br, filename);
                    } else {
                        ignoredFile++;
                        log.error(String.format("Unrecognized file with name '%s' in zip file. Skipped.", filename));
                    }
                } catch (Exception e) {
                    log.error("An error occurred trying to process entry with file name ".concat(filename), e);
                    log.error("This entry will be skipped");
                }
            }

            // Treat nodes in a first time, to fill the idBindingMap for relationships
            for(Map.Entry<BufferedReader, String> pair : nodeBuffers.entrySet()) {
                try {
                    String labelAsString = getLabelFromFilename(pair.getValue());
                    treatNodeBuffer(labelAsString, pair.getKey());
                    countLabelCreated++;
                } catch (FileCorruptedException e) {
                    log.error("The file".concat(pair.getValue()).concat(" seems to be corrupted. Skipped."));
                    ignoredFile++;
                }
            }

            for(Map.Entry<BufferedReader, String> pair : relBuffers.entrySet()) {
                try {
                    String relAsString = getLabelFromFilename(pair.getValue());
                    treatRelBuffer(relAsString, pair.getKey());
                    countRelationTypeCreated++;
                } catch (FileCorruptedException e) {
                    log.error("The file".concat(pair.getValue()).concat(" seems to be corrupted. Skipped."));
                    ignoredFile++;
                }
            }

        } catch (IOException ioException) {
            throw ioException;
        } finally {
            // Close bufferedReader
            for(BufferedReader bf : nodeBuffers.keySet()) bf.close();
            for(BufferedReader bf : relBuffers.keySet()) bf.close();
        }
    }


    /**
     * Neo4 Procedure entry point for "fexporter.load()". See Neo4j documentation for more information.
     * @throws ProcedureException
     */
    @Description("fexporter.load(PathToZipFileName) - Import a configuration zip file to neo4j. \n" +
            "Parameters : \n" +
            "               - @PathToZipFileName - <String> - Location to saved output results. Ex : \"C:\\User\\John\\config.zip\"" +
            "Example of use : CALL fexporter.load(\"C:\\Neo4j_exports\\config.zip\")" +
            "")
    @Procedure(value = "fexporter.load", mode = Mode.WRITE)
    public Stream<OutputMessage> loader(@Name(value = "PathToZipFileName") String pathToZipFileName) throws ProcedureException {

        MESSAGE_QUEUE.clear();

        // Init members ( Neo4J decides sometimes to keep the class instantiated )
        countLabelCreated = 0L;
        countRelationTypeCreated = 0L;
        ignoredFile = 0L;
        nodeCreated = 0L;
        relationshipCreated = 0L;
        idBindingMap = new HashMap<>();

        File zipFile = new File(pathToZipFileName);

        // End the procedure if the path specified isn't valid
        if (!zipFile.exists()) {
            MESSAGE_QUEUE.add(new OutputMessage("No zip file found at path ".concat(pathToZipFileName).concat(". Please check the path provided")));
            return MESSAGE_QUEUE.stream();
        }

        try {
            parseZip(zipFile);
        } catch (IOException e) {
            throw new ProcedureException(e);
        }

        MESSAGE_QUEUE.add(new OutputMessage(String.format("%d file(s) containing a label where found and processed.", countLabelCreated)));
        MESSAGE_QUEUE.add(new OutputMessage(String.format("%d file(s) containing relationships where found and processed.", countRelationTypeCreated)));
        MESSAGE_QUEUE.add(new OutputMessage(String.format("%d file(s) where ignored. Check logs for more information.", ignoredFile)));
        MESSAGE_QUEUE.add(new OutputMessage(String.format("%d node(s) and %d relationship(s) were created during the import.", nodeCreated, relationshipCreated)));

        return MESSAGE_QUEUE.stream();
    }
}
