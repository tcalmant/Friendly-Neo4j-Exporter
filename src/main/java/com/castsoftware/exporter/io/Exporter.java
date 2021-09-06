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
import com.castsoftware.exporter.exceptions.file.FileIOException;
import com.castsoftware.exporter.exceptions.neo4j.Neo4jNoResult;
import com.castsoftware.exporter.exceptions.neo4j.Neo4jQueryException;
import com.castsoftware.exporter.results.OutputMessage;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.neo4j.graphdb.*;
import org.neo4j.logging.Log;

import java.io.*;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public class Exporter {

    /**
     * Output message queue
     */
    private static final List<OutputMessage> MESSAGE_QUEUE = new ArrayList<>();

    // Default properties
    private static final String DELIMITER = IOProperties.Property.CSV_DELIMITER.toString(); // ; | , | space
    private static final String EXTENSION = IOProperties.Property.CSV_EXTENSION.toString(); // .csv
    private static final String INDEX_COL = IOProperties.Property.INDEX_COL.toString(); // Id
    private static final String INDEX_OUTGOING = IOProperties.Property.INDEX_OUTGOING.toString(); // Source
    private static final String INDEX_INCOMING = IOProperties.Property.INDEX_INCOMING.toString(); // Destination
    private static final String RELATIONSHIP_PREFIX = IOProperties.Property.PREFIX_RELATIONSHIP_FILE.toString(); // relationship
    private static final String NODE_PREFIX = IOProperties.Property.PREFIX_NODE_FILE.toString(); // node

    /**
     * Access to Neo4J DB
     */
    private final GraphDatabaseService db;

    /**
     * Neo4J Logger
     */
    private final Log log;

    /**
     * IDs of the nodes visited
     */
    private final Set<Long> nodeLabelMap = new LinkedHashSet<>();

    /**
     * Labels we already have written down
     */
    private final Set<Label> closedLabelSet = new LinkedHashSet<>();

    /**
     * List of the files we created during the visit
     */
    private final Set<String> createdFilenameList = new LinkedHashSet<>();

    /**
     * Lists of the labels we have to visit
     */
    private List<Label> openLabelList = new LinkedList<>();

    /**
     * @param db  Neo4J database access
     * @param log Neo4J logger
     */
    public Exporter(final GraphDatabaseService db, final Log log) {
        this.db = db;
        this.log = log;
    }

    /**
     * Save relationship between found nodes.
     * The saving process will parse relationships a first time to extract all possible properties.
     * Then, it will create associated file, pushes the full set of header values, and write back the relationships.
     * If a relationship doesn't contain a property value, the column for this row will be left empty.
     *
     * @param aTransaction Current DB transaction
     * @param aOutPath     Output folder
     * @param aCSVFormat   Format of the output CSV file
     * @throws FileIOException Error writing file
     */
    private void saveRelationships(final Transaction aTransaction, final Path aOutPath, final CSVFormat aCSVFormat) throws FileIOException, IOException {

        // All relations returned by Neo4J
        final List<Relationship> relationships = new LinkedList<>();

        // Detected header for each relation
        final Map<String, Set<String>> relationshipsHeaders = new HashMap<>();

        // Parse all relationships, extract headers for each relationship
        for (final Long index : nodeLabelMap) {
            final Node node = aTransaction.getNodeById(index);

            for (final Relationship rel : node.getRelationships(Direction.OUTGOING)) {
                final Node otherNode = rel.getOtherNode(node);
                // If the node was saved previously, save its associated relationships
                if (nodeLabelMap.contains(otherNode.getId())) {
                    // Save relationship for later
                    relationships.add(rel);

                    final String relName = rel.getType().name();
                    final List<String> properties = new ArrayList<>();
                    for (final String propName : rel.getPropertyKeys()) {
                        // Extract Iterable to List
                        properties.add(propName);
                    }

                    // Append or create header for this relationship
                    if (relationshipsHeaders.containsKey(relName)) {
                        relationshipsHeaders.get(relName).addAll(properties);
                    } else {
                        relationshipsHeaders.put(relName, new HashSet<>(properties));
                    }
                }
            }
        }

        // Map of the writers: kind of relation -> CSV printer
        final Map<String, CSVPrinter> openedPrinters = new LinkedHashMap<>();
        // Sorted header of each relation
        final Map<String, List<String>> sortedProperties = new HashMap<>();

        try {
            // Sort the headers and prepare the printer
            for (final Map.Entry<String, Set<String>> relHeader : relationshipsHeaders.entrySet()) {
                // Sort the header
                final String relName = relHeader.getKey();
                final List<String> sortedProps = relHeader.getValue().stream().sorted().collect(Collectors.toList());
                sortedProperties.put(relName, sortedProps);

                // Prepare the printer
                final String filename = RELATIONSHIP_PREFIX + relName + EXTENSION;
                final File outFile = aOutPath.resolve(filename).toFile();
                final CSVPrinter csvPrinter = new CSVPrinter(new FileWriter(outFile), aCSVFormat);
                openedPrinters.put(relName, csvPrinter);

                // Write the header
                csvPrinter.print(INDEX_OUTGOING);
                csvPrinter.print(INDEX_INCOMING);
                for (final String propName : sortedProps) {
                    csvPrinter.print(propName);
                }
                csvPrinter.println();
            }

            // Write all relations
            for (final Relationship rel : relationships) {
                // Get the name of the relation
                final String relName = rel.getType().name();

                // Find the related information
                final CSVPrinter csvPrinter = openedPrinters.get(relName);
                final List<String> sortedProps = sortedProperties.get(relName);

                // Print the edge
                csvPrinter.print(rel.getStartNodeId());
                csvPrinter.print(rel.getEndNodeId());

                // Add properties
                for (final String propName : sortedProps) {
                    try {
                        csvPrinter.print(rel.getProperty(propName));
                    } catch (NotFoundException ex) {
                        // Property not used here
                        csvPrinter.print(null);
                    }
                }

                // End of relation
                csvPrinter.println();
            }
        } finally {
            for (final CSVPrinter csvPrinter : openedPrinters.values()) {
                try {
                    csvPrinter.close();
                } catch (IOException ex) {
                    log.error("Error closing CSV printer", ex);
                }
            }
        }
    }

    /**
     * Explore neighborhood of specified node and extract potentials neighbors' label.
     * New labels will be added to the open list
     *
     * @param node Node to study
     */
    private void handleNeighbors(final Node node) {
        for (final Relationship rel : node.getRelationships()) {
            final Node otherNode = rel.getOtherNode(node);

            // Retrieve all neighbor's labels
            for (final Label label : otherNode.getLabels()) {
                // If the label wasn't already visited or not already appended, add to open list
                if (!closedLabelSet.contains(label) && !openLabelList.contains(label)) {
                    openLabelList.add(label);
                }
            }
        }
    }

    /**
     * Convert a node list associated to the given label into a CSV format.
     * If the option @ConsiderNeighbors is active, neighbors label found will be added to the discovery list.
     *
     * @param aTransaction      Current DB transaction
     * @param printer           The CSV printer
     * @param label             The label to save
     * @param considerNeighbors If True, analyze the neighbouring nodes
     * @throws Neo4jNoResult       No node with the label provided where found during parsing
     * @throws Neo4jQueryException Error while querying the database
     * @throws IOException         Error writing down the CSV file
     */
    private void exportLabelToCSV(final Transaction aTransaction, final CSVPrinter printer, final Label label, final boolean considerNeighbors) throws Neo4jNoResult, Neo4jQueryException, IOException {
        final Set<String> propertiesSet = new HashSet<>();
        final List<Node> nodeList = new ArrayList<>();

        // Find all nodes of that label
        ResourceIterator<Node> nodeIt;
        try {
            nodeIt = aTransaction.findNodes(label);
        } catch (Exception e) {
            throw new Neo4jQueryException("An error occurred trying to retrieve node by label", e, "SAVExELTC01");
        }

        while (nodeIt != null && nodeIt.hasNext()) {
            final Node node = nodeIt.next();
            // Retrieve all possible node property keys
            for (final String propName : node.getPropertyKeys()) {
                propertiesSet.add(propName);
            }
            nodeList.add(node);
        }

        // If no nodes were found, end with exception
        if (nodeList.isEmpty()) {
            throw new Neo4jNoResult("No result for findNodes with label " + label.name(),
                    "findNodes(" + label.name() + ");", "SAVExELTC02");
        }

        // Write the header
        final List<String> sortedProps = propertiesSet.stream().sorted().collect(Collectors.toList());
        final List<String> fullHeader = new ArrayList<>(propertiesSet.size() + 1);
        fullHeader.add(INDEX_COL);
        fullHeader.addAll(sortedProps);
        printer.printRecord(fullHeader);

        log.info("Appending headers for label " + label.name());

        for (final Node node : nodeList) {
            // Print the index
            final long nodeId = node.getId();
            printer.print(nodeId);
            nodeLabelMap.add(nodeId);

            // Print the properties
            for (final String property : sortedProps) {
                try {
                    printer.print(node.getProperty(property));
                } catch (NotFoundException ex) {
                    printer.print(null);
                }
            }
            // End of the node
            printer.println();

            if (considerNeighbors) {
                handleNeighbors(node);
            }
        }

        // Mark the label as visited
        closedLabelSet.add(label);
    }

    /**
     * Appends all the files created during this process to the target zip.
     * Every file appended will be remove once added to the zip.
     *
     * @param csvFolder Folder where to find the CSV files
     * @param outputZip Path to the output ZIP file
     * @throws FileIOException Error writing ZIP file / reading CSV file
     */
    private void createZip(final Path csvFolder, final Path outputZip) throws FileIOException {
        log.info("Creating zip file..");
        try (final ZipOutputStream zipOut = new ZipOutputStream(new FileOutputStream(outputZip.toFile()))) {
            for (final String filename : createdFilenameList) {
                final File fileToZip = csvFolder.resolve(filename).toFile();

                try (final FileInputStream fileStream = new FileInputStream(fileToZip)) {
                    final ZipEntry zipEntry = new ZipEntry(filename);
                    zipOut.putNextEntry(zipEntry);

                    final byte[] bytes = new byte[1024];
                    int length;
                    while ((length = fileStream.read(bytes)) >= 0) {
                        zipOut.write(bytes, 0, length);
                    }
                } catch (Exception e) {
                    log.error("An error occurred trying to zip file with name: " + filename, e);
                }

                if (!fileToZip.delete()) {
                    log.error("Error trying to delete file with name: " + filename);
                }
            }

        } catch (IOException e) {
            log.error("An error occurred trying create zip file with name: " + outputZip.getFileName(), e);
            throw new FileIOException("An error occurred trying create zip file with name.", e, "SAVExCZIP01");
        }

    }

    /**
     * Iterate through label list, find associated nodes and export them to a CSV file.
     *
     * @param aTransaction       Current DB transaction
     * @param aOutputPath        Path where to write the CSV files
     * @param aCSVFormat         Format of the output CSV files
     * @param aConsiderNeighbors If true, analyze neighboring nodes
     * @throws FileIOException Error writing file
     */
    private void saveNodes(final Transaction aTransaction, final Path aOutputPath, final CSVFormat aCSVFormat, final boolean aConsiderNeighbors) throws FileIOException {
        while (!openLabelList.isEmpty()) {
            final Label toTreat = openLabelList.remove(0);

            // Compute the name of the output file
            final String filename = NODE_PREFIX + toTreat.name() + EXTENSION;
            final File outFile = aOutputPath.resolve(filename).toFile();
            createdFilenameList.add(filename);

            try (final CSVPrinter printer = new CSVPrinter(new FileWriter(outFile), aCSVFormat)) {
                exportLabelToCSV(aTransaction, printer, toTreat, aConsiderNeighbors);
            } catch (Neo4jNoResult | Neo4jQueryException ex) {
                log.error("Error trying to save label: " + toTreat.name(), ex);
                MESSAGE_QUEUE.add(new OutputMessage("Error : No nodes found with label : ".concat(toTreat.name())));
            } catch (IOException ex) {
                throw new FileIOException("Error: Can't create/open file with name " + filename, ex, "SAVExSAVE01");
            }
        }
    }

    /**
     * Entrypoint of the exporter
     *
     * @param labelList         Labels to write down (initial set)
     * @param path              Output path (folder)
     * @param zipFileName       Name of the output ZIP file
     * @param saveRelationShip  If True, save the relationship in addition to the nodes
     * @param considerNeighbors If True, analyze neighbors instead of the given labels only
     * @return The message to output to Neo4J
     * @throws ProcedureException An error occurred while querying/reading/writing
     */
    public Stream<OutputMessage> save(final List<String> labelList,
                                      final String path,
                                      final String zipFileName,
                                      final boolean saveRelationShip,
                                      final boolean considerNeighbors) throws ProcedureException {
        MESSAGE_QUEUE.clear();

        // Prepare the output folder
        final Path outputPath = Paths.get(path);
        try {
            Files.createDirectories(outputPath);
        } catch (FileAlreadyExistsException ignored) {
            // Folder already there: use it
        } catch (IOException ex) {
            throw new ProcedureException("Error creating output folder", ex);
        }

        // Initial labels to grab
        openLabelList = labelList.stream().map(Label::label).collect(Collectors.toList());

        // Update the CSVFormat
        CSVFormat csvFormat = CSVFormat.EXCEL;
        if (DELIMITER != null && DELIMITER.length() == 1) {
            csvFormat = csvFormat.withDelimiter(DELIMITER.charAt(0));
        }

        // openTransaction
        try (final Transaction tx = db.beginTx()) {
            saveNodes(tx, outputPath, csvFormat, considerNeighbors);
            if (saveRelationShip) {
                saveRelationships(tx, outputPath, csvFormat);
            }

            if (zipFileName == null || zipFileName.isEmpty()) {
                MESSAGE_QUEUE.add(new OutputMessage("CSV files created."));
            } else {
                final String targetZipName = zipFileName + ".zip";
                log.info(String.format("Saving Configuration to %s ...", targetZipName));

                createZip(outputPath, outputPath.resolve(targetZipName));
                MESSAGE_QUEUE.add(new OutputMessage("ZIP file created."));
            }

            // Commit the transaction
            tx.commit();

            return MESSAGE_QUEUE.stream();
        } catch (FileIOException e) {
            throw new ProcedureException(e);
        } catch (IOException ex) {
            throw new ProcedureException(ex.getMessage(), ex);
        }
    }
}
