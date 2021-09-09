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


package com.castsoftware.exporter.io;

import com.castsoftware.exporter.exceptions.ProcedureException;
import com.castsoftware.exporter.exceptions.file.FileCorruptedException;
import com.castsoftware.exporter.exceptions.neo4j.Neo4jQueryException;
import com.castsoftware.exporter.results.OutputMessage;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.neo4j.graphdb.*;
import org.neo4j.logging.Log;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
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

public class Importer {

    // Default values ( Should be added to a property file )
    private final String DELIMITER = IOProperties.Property.CSV_DELIMITER.toString();
    private final String EXTENSION = IOProperties.Property.CSV_EXTENSION.toString();
    private final String INDEX_COL = IOProperties.Property.INDEX_COL.toString();
    private final String INDEX_OUTGOING = IOProperties.Property.INDEX_OUTGOING.toString();
    private final String INDEX_INCOMING = IOProperties.Property.INDEX_INCOMING.toString();
    private final String RELATIONSHIP_PREFIX = IOProperties.Property.PREFIX_RELATIONSHIP_FILE.toString();
    private final String NODE_PREFIX = IOProperties.Property.PREFIX_NODE_FILE.toString();

    /**
     * Output message queue
     */
    private final List<OutputMessage> MESSAGE_QUEUE = new ArrayList<>();

    /**
     * Neo4J database
     */
    private final GraphDatabaseService db;

    /**
     * Neo4J logger
     */
    private final Log log;

    /**
     * Binding map between csv ID and Neo4j created nodes.
     * Only the Node id is stored here, to limit the usage of heap memory.
     */
    private final Map<Long, Long> idBindingMap = new LinkedHashMap<>();

    /**
     * Number of labels we created
     */
    private long countLabelCreated;

    /**
     * Number of relation types we created
     */
    private long countRelationTypeCreated;

    /**
     * Number of files ingnored in the Zip file
     */
    private long ignoredFile;

    /**
     * Number of nodes created
     */
    private long nodeCreated;

    /**
     * Number of relations created
     */
    private long relationshipCreated;

    /**
     * Sets up the importer
     *
     * @param db  Access to the database
     * @param log Access to the logger
     */
    public Importer(final GraphDatabaseService db, final Log log) {
        this.db = db;
        this.log = log;

        // Init members
        countLabelCreated = 0L;
        countRelationTypeCreated = 0L;
        ignoredFile = 0L;
        nodeCreated = 0L;
        relationshipCreated = 0L;
    }

    /**
     * Convert a String containing a Neo4j Type to a Java Type.
     * Handled type are: <code>Boolean, Char, Long, Double, LocalDate, OffsetTime, LocalTime, ZoneDateTime</code>.
     * <p>
     * If none of these types are detected, it will return the value as a String.
     * <p>
     * <strong>Warning:</strong> <code>TemporalAmount</code> and <code>org.neo4j.graphdb.spatial.Point</code> are not detected
     * Check <a href="https://neo4j.com/docs/java-reference/current/java-embedded/property-values/index.html for more information">Neo4J property values documentation</a>
     * </p>
     * <p>
     * <strong>Warning:</strong> The goal of this function is to reassign the correct Java type to the value discovered in the CSV. It mays detect the wrong type.
     * </p>
     *
     * @param value Neo4j Value as a string
     * @return Object of the Java Type associated to the discovered type within the string provided
     */
    private Object getNeo4jType(final String value) {

        // Long
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException ignored) {
        }

        // Double
        try {
            return Double.parseDouble(value);
        } catch (NumberFormatException ignored) {
        }

        // Boolean
        if (value.toLowerCase().matches("true|false")) {
            return Boolean.parseBoolean(value);
        }

        // DateTimeFormatter covering all Neo4J Date Format
        // (cf: https://neo4j.com/docs/cypher-manual/current/syntax/temporal/)
        // FIXME: quarter and week base ones are wrong
        final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("[yyyy-MM-dd]" +
                "[yyyyMMdd]" +
                "[yyyy-MM]" +
                "[yyyyMM]" +
                "[YYYY-'W'ww-D]" +
                "[YYYY-'W'wwD]" +
                "[YYYY-'W'ww]" +
                "[YYYY'W'ww]" +
                "[YYYY-'Q'Q-DD]" +
                "[YYYY-'Q'q]" +
                "[YYYY'Q'q]" +
                "[yyyy-DDD]" +
                "[yyyyDDD]" +
                "[yyyy]");

        // LocalDate
        try {
            return LocalDate.parse(value, formatter);
        } catch (DateTimeParseException ignored) {
        }

        // OffsetTime
        try {
            return OffsetTime.parse(value, formatter);
        } catch (DateTimeParseException ignored) {
        }

        // LocalTime
        try {
            return LocalTime.parse(value, formatter);
        } catch (DateTimeParseException ignored) {
        }

        // ZoneDateTime
        try {
            return ZonedDateTime.parse(value, formatter);
        } catch (DateTimeParseException ignored) {
        }

        // Char
        if (value.length() == 1) {
            return value.charAt(0);
        }

        // Remove Sanitization
        final String sanitized = value.replaceAll("(^\\s\")|(\\s\"\\s?$)", "");

        log.info("Value inserted: " + sanitized);
        // String
        return sanitized;
    }

    /**
     * Get the label stored within the filename by removing the prefix and the extension
     *
     * @param filename Name of the input file
     * @return The name of the Label
     */
    private String getLabelFromFilename(final String filename) {
        return filename.replace(RELATIONSHIP_PREFIX, "")
                .replace(NODE_PREFIX, "")
                .replace(EXTENSION, "");
    }

    /**
     * Create a Node based on provided header and values.
     * <p>
     * If a value is empty, it won't be added as a property to the node.
     * <p>
     * To make the <code>com.castsoftware.exporter</code> more generic, the conversion from CSV
     * values to Java Values does not necessitate POJOs object.
     * However, the drawback is that this conversion can create some errors.
     * See {@link #getNeo4jType(String)} for more information.
     *
     * @param aTx     Current transaction
     * @param aLabel  Label that will be given to the node
     * @param aRecord Record as parsed from the CSV file
     */
    private void createNode(final Transaction aTx, final Label aLabel, final CSVRecord aRecord)
            throws Neo4jQueryException {
        // Get the node ID
        final long id = Long.parseLong(aRecord.get(INDEX_COL));

        try {
            final Node node = aTx.createNode(aLabel);

            for (final Map.Entry<String, String> pair : aRecord.toMap().entrySet()) {
                final String propName = pair.getKey();
                if (INDEX_COL.equals(propName)) {
                    // Ignore the index column
                    continue;
                }

                // Ignore empty values
                final String propValue = pair.getValue();
                if (propValue.isEmpty()) {
                    continue;
                }

                // Parse the string
                final Object parsedValue = getNeo4jType(propValue);
                node.setProperty(propName, parsedValue);
            }

            nodeCreated++;

            // We need to keep a track of the CSV ID to bind nodes together later
            idBindingMap.put(id, node.getId());
        } catch (Exception e) {
            throw new Neo4jQueryException("Node creation failed.", e, "IMPOxCREN01");
        }
    }

    /**
     * Creates a relationship between two node.
     * <p>
     * Source node ID and Destination node must be specified in the header and the value list.
     * If this information is missing, the relationship will be ignored.
     *
     * @param aTx               Current transaction
     * @param aRelationshipType The name of the relationship
     * @param aRecord           Parsed CSV record
     */
    private void createRelationship(
            final Transaction aTx,
            final RelationshipType aRelationshipType,
            final CSVRecord aRecord) throws Neo4jQueryException {

        final Long idOutgoing = Long.parseLong(aRecord.get(INDEX_OUTGOING));
        final Long idIncoming = Long.parseLong(aRecord.get(INDEX_INCOMING));
        final Long srcNodeId = idBindingMap.get(idOutgoing);
        final Long destNodeId = idBindingMap.get(idIncoming);

        if (srcNodeId == null || destNodeId == null) {
            // Ignore this relationship, at least one node is missing
            return;
        }

        // Get the Neo4J nodes
        final Node srcNode;
        final Node destNode;
        try {
            srcNode = aTx.getNodeById(srcNodeId);
            destNode = aTx.getNodeById(destNodeId);
        } catch (Exception e) {
            throw new Neo4jQueryException("Impossible to retrieve Dest/Src Node.", e, "IMPOxCRER01");
        }

        // Get the Neo4J relationship
        final Relationship rel = srcNode.createRelationshipTo(destNode, aRelationshipType);

        for (final Map.Entry<String, String> pair : aRecord.toMap().entrySet()) {
            final String propName = pair.getKey();
            if (INDEX_INCOMING.equals(propName) || INDEX_OUTGOING.equals(propName)) {
                // Ignore the index columns
                continue;
            }

            // Ignore empty values
            final String propValue = pair.getValue();
            if (propValue.isEmpty()) {
                continue;
            }

            // Parse the string
            final Object parsedValue = getNeo4jType(propValue);
            rel.setProperty(propName, parsedValue);
        }

        relationshipCreated++;
    }

    /**
     * Treat a node buffer by extracting the first row as a list of header value. Treat all the other rows as list of node's values.
     *
     * @param aTx             Current transaction
     * @param associatedLabel Name of the label
     * @param nodeFileReader  BufferReader pointing to the node file
     * @param aCsvFormat      Format of the input CSV
     * @throws IOException            thrown if the procedure fails to read the buffer
     * @throws FileCorruptedException thrown if the file isn't in a good format ( If the headers are missing, or if it does not contains any Index Column)
     */
    private void treatNodeBuffer(
            final Transaction aTx, final String associatedLabel,
            final BufferedReader nodeFileReader, final CSVFormat aCsvFormat)
            throws IOException, FileCorruptedException {

        try (final CSVParser parser = new CSVParser(nodeFileReader, aCsvFormat)) {
            // Get the header
            final List<String> headerList = parser.getHeaderNames();
            if (headerList.isEmpty()) {
                throw new FileCorruptedException("No header found in file.", "LOADxTNBU01");
            }

            //Process header line
            if (!headerList.contains(INDEX_COL)) {
                throw new FileCorruptedException("No index column found in file.", "LOADxTNBU02");
            }

            // Prepare the label
            final Label label = Label.label(associatedLabel);

            // Load the CSV file
            for (final CSVRecord row : parser) {
                try {
                    createNode(aTx, label, row);
                } catch (Exception | Neo4jQueryException e) {
                    log.error("An error occurred during creation of node with label: "
                            + associatedLabel + " and values: " + row.toMap(), e);
                }
            }
        }
    }

    /**
     * Treat a relationship buffer by extracting the first row as a list of header value.
     * Treat all the other rows as list of relationship's values.
     *
     * @param aTx                Current transaction
     * @param associatedRelation Name of the relationship
     * @param relFileReader      BufferReader pointing to the relationship file
     * @param aCsvFormat         Format of the input CSV file
     * @throws IOException            thrown if the procedure fails to read the buffer
     * @throws FileCorruptedException thrown if the file isn't in a good format
     *                                (If the headers are missing, or if it does not contain
     *                                any Source or Destination index column)
     */
    private void treatRelBuffer(
            final Transaction aTx,
            final String associatedRelation,
            final BufferedReader relFileReader,
            final CSVFormat aCsvFormat) throws IOException, FileCorruptedException, Neo4jQueryException {

        try (final CSVParser parser = new CSVParser(relFileReader, aCsvFormat)) {
            // Get the header
            final List<String> headerList = parser.getHeaderNames();
            if (headerList.isEmpty()) {
                throw new FileCorruptedException("No header found in file.", "LOADxTNBU01");
            }

            if (!headerList.contains(INDEX_OUTGOING) || !headerList.contains(INDEX_INCOMING)) {
                // Missing headers
                throw new FileCorruptedException(
                        "Corrupted header (missing source or destination columns).",
                        "LOADxTNBU02");
            }

            // Prepare the relationship
            final RelationshipType relationship = RelationshipType.withName(associatedRelation);

            // Load the CSV file
            for (final CSVRecord row : parser) {
                createRelationship(aTx, relationship, row);
            }
        }
    }

    /**
     * Parse all files within zip file.
     * <p>
     * For each file a BufferedReader will be open and stored as a Node BufferReader or
     * a Relationship BufferReader.
     * The procedure will use the prefix in the filename to decide if it must be treated as
     * a file containing node or relationships.
     *
     * @param tx         Current transaction
     * @param zipPath    The Zip file to be treated
     * @param aCsvFormat Format of the input CSV files
     * @throws IOException         Error reading the ZIP file
     * @throws Neo4jQueryException Error creating the nodes/relationships
     */
    private void parseZip(final Transaction tx, final Path zipPath, final CSVFormat aCsvFormat) throws IOException, Neo4jQueryException {
        final Map<BufferedReader, String> nodeBuffers = new LinkedHashMap<>();
        final Map<BufferedReader, String> relBuffers = new LinkedHashMap<>();

        try (final ZipFile zf = new ZipFile(zipPath.toFile())) {
            final Enumeration<? extends ZipEntry> entries = zf.entries();
            while (entries.hasMoreElements()) {
                final ZipEntry zipEntry = entries.nextElement();
                final String filename = zipEntry.getName();
                if (zipEntry.getSize() < 0) {
                    // Invalid entry (unknown uncompressed size)
                    continue;
                }

                if (!filename.endsWith(EXTENSION) ||
                        !(filename.startsWith(RELATIONSHIP_PREFIX)) || filename.startsWith(NODE_PREFIX)) {
                    // Filter out files we don't know
                    ignoredFile++;
                    log.error(String.format("Unrecognized file with name '%s' in zip file. Skipped.", filename));
                    continue;
                }

                try {
                    // Open a buffered reader for this file
                    final BufferedReader br = new BufferedReader(new InputStreamReader(zf.getInputStream(zipEntry)));
                    if (filename.startsWith(RELATIONSHIP_PREFIX)) {
                        relBuffers.put(br, filename);
                    } else if (filename.startsWith(NODE_PREFIX)) {
                        nodeBuffers.put(br, filename);
                    }
                } catch (Exception e) {
                    log.error("An error occurred trying to process entry with file name ".concat(filename), e);
                    log.error("This entry will be skipped");
                }
            }

            // Load the data
            loadData(tx, aCsvFormat, nodeBuffers, relBuffers);
        }
    }

    /**
     * Loads a folder containing CSV files
     *
     * @param aTx         Current transaction
     * @param aFolderPath Input folder
     * @param aCsvFormat  CSV format of the input files
     * @throws IOException         Error reading files
     * @throws Neo4jQueryException Error creating the nodes/relationships
     */
    private void loadFolder(final Transaction aTx, final Path aFolderPath, final CSVFormat aCsvFormat) throws IOException, Neo4jQueryException {

        final Map<BufferedReader, String> nodeBuffers = new LinkedHashMap<>();
        final Map<BufferedReader, String> relBuffers = new LinkedHashMap<>();
        final List<Path> erroneous = new LinkedList<>();

        Files.list(aFolderPath).filter((path) -> {
            final String filename = path.getFileName().toString();
            return filename.endsWith(EXTENSION) &&
                    (filename.startsWith(RELATIONSHIP_PREFIX) || filename.startsWith(NODE_PREFIX));
        }).forEach((path) -> {
            try {
                final BufferedReader br = Files.newBufferedReader(path);
                final String filename = path.getFileName().toString();
                if (filename.startsWith(RELATIONSHIP_PREFIX)) {
                    relBuffers.put(br, filename);
                } else if (filename.startsWith(NODE_PREFIX)) {
                    nodeBuffers.put(br, filename);
                }
            } catch (IOException ex) {
                erroneous.add(path);
                MESSAGE_QUEUE.add(new OutputMessage("Error opening file: " + path + " - " + ex));
            }
        });

        if (erroneous.isEmpty()) {
            // All files were opened, load the data
            loadData(aTx, aCsvFormat, nodeBuffers, relBuffers);
        }
    }

    /**
     * Loads data from the given readers and closes them once done or in case of an error
     *
     * @param aTx         Current transaction
     * @param aCsvFormat  Format of the input CSV files
     * @param nodeBuffers Opened buffers for the nodes
     * @param relBuffers  Opened buffers for the relations
     * @throws IOException         Error reading files
     * @throws Neo4jQueryException Error creating nodes/relations
     */
    private void loadData(
            final Transaction aTx,
            final CSVFormat aCsvFormat,
            final Map<BufferedReader, String> nodeBuffers,
            final Map<BufferedReader, String> relBuffers) throws IOException, Neo4jQueryException {

        try {
            // Treat nodes in a first time, to fill the idBindingMap for relationships
            for (final Map.Entry<BufferedReader, String> pair : nodeBuffers.entrySet()) {
                final String filename = pair.getValue();
                try {
                    final String labelAsString = getLabelFromFilename(filename);
                    treatNodeBuffer(aTx, labelAsString, pair.getKey(), aCsvFormat);
                    countLabelCreated++;
                } catch (FileCorruptedException e) {
                    log.error("The file" + filename + " seems to be corrupted. Skipped.");
                    ignoredFile++;
                }
            }

            for (final Map.Entry<BufferedReader, String> pair : relBuffers.entrySet()) {
                final String filename = pair.getValue();
                try {
                    final String relAsString = getLabelFromFilename(filename);
                    treatRelBuffer(aTx, relAsString, pair.getKey(), aCsvFormat);
                    countRelationTypeCreated++;
                } catch (FileCorruptedException e) {
                    log.error("The file " + filename + " seems to be corrupted. Skipped.");
                    ignoredFile++;
                } catch (Neo4jQueryException e) {
                    log.error("Operation failed, check the stack trace for more information.");
                    throw e;
                }
            }
        } finally {
            // Close readers
            for (final BufferedReader bf : nodeBuffers.keySet()) {
                bf.close();
            }

            for (final BufferedReader bf : relBuffers.keySet()) {
                bf.close();
            }
        }
    }

    /**
     * Entrypoint: load the given CSV files (from a Zip file or a folder)
     *
     * @param zipOrFolderName Name of the Zip file or folder containing the CSV files
     * @return Messages to output to the user
     * @throws ProcedureException Error loading the data
     */
    public Stream<OutputMessage> load(final String zipOrFolderName) throws ProcedureException {
        MESSAGE_QUEUE.clear();

        // Prepare the CSV format
        CSVFormat csvFormat = CSVFormat.EXCEL;
        if (DELIMITER != null && DELIMITER.length() == 1) {
            csvFormat = csvFormat.withDelimiter(DELIMITER.charAt(0));
        }

        try (final Transaction tx = db.beginTx()) {
            final Path inputPath = Paths.get(zipOrFolderName);
            if (Files.notExists(inputPath)) {
                // File/folder not found
                MESSAGE_QUEUE.add(new OutputMessage(
                        "Input file or folder not found at: " + inputPath +
                                ". Please check the provided path"));
                return MESSAGE_QUEUE.stream();
            }

            if (Files.isDirectory(inputPath)) {
                // Load CSV files from a path
                loadFolder(tx, inputPath, csvFormat);
            } else {
                // Load CSV files from a Zip file
                parseZip(tx, inputPath, csvFormat);
            }

            // Commit if we succeeded
            tx.commit();
        } catch (IOException | Neo4jQueryException e) {
            throw new ProcedureException(e);
        }

        MESSAGE_QUEUE.add(new OutputMessage(String.format("%d file(s) containing a label where found and processed.", countLabelCreated)));
        MESSAGE_QUEUE.add(new OutputMessage(String.format("%d file(s) containing relationships where found and processed.", countRelationTypeCreated)));
        MESSAGE_QUEUE.add(new OutputMessage(String.format("%d file(s) where ignored. Check logs for more information.", ignoredFile)));
        MESSAGE_QUEUE.add(new OutputMessage(String.format("%d node(s) and %d relationship(s) were created during the import.", nodeCreated, relationshipCreated)));

        return MESSAGE_QUEUE.stream();
    }
}
