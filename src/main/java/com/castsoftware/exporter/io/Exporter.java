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
import org.neo4j.graphdb.*;
import org.neo4j.logging.Log;

import java.io.*;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public class Exporter {

    // Return message queue
    private static final List<OutputMessage> MESSAGE_QUEUE = new ArrayList<>();

    // Default properties
    private static final String DELIMITER = IOProperties.Property.CSV_DELIMITER.toString(); // ; | , | space
    private static final String EXTENSION = IOProperties.Property.CSV_EXTENSION.toString(); // .csv
    private static final String INDEX_COL = IOProperties.Property.INDEX_COL.toString(); // Id
    private static final String INDEX_OUTGOING = IOProperties.Property.INDEX_OUTGOING.toString(); // Source
    private static final String INDEX_INCOMING = IOProperties.Property.INDEX_INCOMING.toString(); // Destination
    private static final String RELATIONSHIP_PREFIX = IOProperties.Property.PREFIX_RELATIONSHIP_FILE.toString(); // relationship
    private static final String NODE_PREFIX = IOProperties.Property.PREFIX_NODE_FILE.toString(); // node

    private GraphDatabaseService db;
    private Log log;
    private Transaction tx;

    // Parameters
    private Boolean saveRelationshipParams = false;
    private Boolean considerNeighborsParams = false;
    private String pathParams = null;

    // Class members
    private Set<Long> nodeLabelMap; // List of node Id visited
    private Set<Label> closedLabelSet; // Already visited Node labels
    private List<Label> openLabelList = null; // To visit Node labels
    private Set<String> createdFilenameList; // Filename created during this session

    /**
     * Save relationship between found nodes.
     * The saving process will parse relationships a first time to extract all possible properties.
     * Then, it will create associated file, pushes the full set of header values, and write back the relationships.
     * If a relationship doesn't contain a property value, the column for this row will be left empty.
     * @throws FileIOException
     */
    private void saveRelationships() throws FileIOException {

        Map<String, FileWriter> fileWriterMap = new HashMap<>();

        try  {
            ArrayList<Relationship> relationships = new ArrayList<>();
            Map<String, Set<String>> relationshipsHeaders = new HashMap<>();

            // Parse all relationships, extract headers for each relations
            for (Long index : nodeLabelMap) {

                Node node = this.tx.getNodeById(index);

                for (Relationship rel : node.getRelationships(Direction.OUTGOING)) {
                    Node otherNode = rel.getOtherNode(node);
                    // If the node was saved previously, save its associated relationships
                    if (nodeLabelMap.contains(otherNode.getId())) {
                        relationships.add(rel); // Save relationship for later

                        String name = rel.getType().name();
                        List<String> properties = new ArrayList<>();
                        for (String prop : rel.getPropertyKeys()) properties.add(prop); // Extract Iterable to List

                        // Append or create header for this relationship
                        if (relationshipsHeaders.containsKey(name)) {
                            relationshipsHeaders.get(name).addAll(properties);
                        } else {
                            relationshipsHeaders.put(name, new HashSet<>(properties));
                        }
                    }
                }
            }

            // Open one FileWriter per name and append headers
            for (Map.Entry<String, Set<String>> pair : relationshipsHeaders.entrySet()) {

                String filename = RELATIONSHIP_PREFIX.concat(pair.getKey()).concat(EXTENSION);

                try {
                    // Create a new file writer and write headers for each relationship type
                    FileWriter writer = new FileWriter(pathParams.concat(filename), true);
                    fileWriterMap.put(pair.getKey(), writer);
                    createdFilenameList.add(filename);

                    StringBuilder headers = new StringBuilder();
                    headers.append(INDEX_OUTGOING.concat(DELIMITER)); // Add Source property
                    headers.append(INDEX_INCOMING.concat(DELIMITER)); // Add Destination property
                    headers.append(String.join(DELIMITER, pair.getValue())).append("\n");

                    writer.write(headers.toString());
                } catch (IOException e) {
                    log.error("Error : Impossible to create/open file with name ".concat(filename), e);
                }
            }

            // Parse previously saved relationships and write them back to their associated FileWriter
            for(Relationship rel : relationships) {
                String name = rel.getType().name();

                Set<String> headers = relationshipsHeaders.get(name);
                StringBuilder values = new StringBuilder();

                List<String> valueList = new ArrayList<>();
                // Append Source and destination nodes ID
                Long idSrc = rel.getStartNode().getId();
                Long idDest = rel.getEndNode().getId();
                valueList.add(idSrc.toString());
                valueList.add(idDest.toString());

                // Append rest of the properties
                for (String prop : headers) {
                    String value = "";
                    try {
                        value = rel.getProperty(prop).toString();
                    } catch (NotFoundException ignored) {
                    }
                    valueList.add(value);
                }

                fileWriterMap.get(name).write(String.join(DELIMITER, valueList).concat("\n"));
            }

        } catch (IOException rethrown ) {
            throw new FileIOException("Error while saving relationships", rethrown, "SAVExSARE01");
        } finally {
            // Close FileWriters
            for (Map.Entry<String, FileWriter> pair : fileWriterMap.entrySet()) {
                try {
                    pair.getValue().close();
                } catch (IOException e) {
                    log.error("Error : Impossible to close file with name ".concat(pair.getKey()), e);
                }
            }
        }
    }

    /**
     * Explore neighborhood of specified node and extract potentials neighbors' label.
     * New labels will be added to the open list
     * @param n Node to study
     */
    private void handleNeighbors(Node n) {
        for (Relationship rel : n.getRelationships()) {
            Node otherNode = rel.getOtherNode(n);

            // Retrieve all neighbor's labels
            for (Label l : otherNode.getLabels()) {
                // If the label wasn't already visited or not already appended, add to open list
                if (!closedLabelSet.contains(l) && !openLabelList.contains(l)) {
                    openLabelList.add(l);
                }
            }
        }

    }

    /**
     * Convert a node list associated to the given label into a CSV format.
     * If the option @ConsiderNeighbors is active, neighbors label found will be added to the discovery list.
     * @param label The label to save
     * @return <code>String</code> the list of node as CSV
     * @throws Neo4jNoResult No node with the label provided where found during parsing
     */
    private String exportLabelToCSV(Label label) throws Neo4jNoResult, Neo4jQueryException {
        Set<String> headers = new HashSet<>();
        List<Node> nodeList = new ArrayList<>();

        ResourceIterator<Node> nodeIt = null;

        try {
            nodeIt = this.tx.findNodes(label);
        } catch (Exception e) {
            throw new Neo4jQueryException("An error occured trying to retrieve node by label", e, "SAVExELTC01");
        }

        while (nodeIt != null && nodeIt.hasNext()) {
            Node n = nodeIt.next();
            // Retrieve all possible node property keys
            for (String s : n.getPropertyKeys()) headers.add(s);
            nodeList.add(n);
        }

        // If no nodes were found, end with exception
        if (nodeList.isEmpty())
            throw new Neo4jNoResult("No result for findNodes with label".concat(label.name()), "findNodes(".concat(label.name()).concat(");"), "SAVExELTC02");

        // Create CSV string
        StringBuilder csv = new StringBuilder();
        csv.append(INDEX_COL.concat(DELIMITER)); // Add index property
        csv.append(String.join(DELIMITER, headers)).append("\n");

        log.info("Appending headers for label ".concat(label.name()));

        for (Node n : nodeList) {
            List<String> valueList = new ArrayList<>();

            // Using the Neo4j Node ID
            valueList.add(((Long) n.getId()).toString());
            nodeLabelMap.add(n.getId());

            for (String prop : headers) {
                String value = "";
                try {
                    value = n.getProperty(prop).toString();

                    // If a semi-colon is present in the value, sanitize the results
                    if(value.contains(";")) {
                        value = String.format("\"%s\"", value);
                    }

                } catch (NotFoundException ignored) {
                }
                valueList.add(value);
            }
            csv.append(String.join(DELIMITER, valueList)).append("\n");

            if(considerNeighborsParams) handleNeighbors(n);
        }

        // Mark the label as visited
        closedLabelSet.add(label);
        return csv.toString();
    }

    /**
     * Appends all the files created during this process to the target zip.
     * Every file appended will be remove once added to the zip.
     * @param targetName Name of the ZipFile
     * @throws IOException
     */
    private void createZip(String targetName) throws FileIOException {
        File f = new File(pathParams.concat(targetName));
        log.info("Creating zip file..");

        try (ZipOutputStream zipOut = new ZipOutputStream(new FileOutputStream(f))) {

            for(String filename : createdFilenameList) {
                File fileToZip = new File(pathParams.concat(filename));

                try (FileInputStream fileStream = new FileInputStream(fileToZip)){
                    ZipEntry e = new ZipEntry(filename);
                    zipOut.putNextEntry(e);

                    byte[] bytes = new byte[1024];
                    int length;
                    while((length = fileStream.read(bytes)) >= 0) {
                        zipOut.write(bytes, 0, length);
                    }

                } catch (Exception e) {
                    log.error("An error occurred trying to zip file with name : ".concat(filename), e);
                }

                if(!fileToZip.delete()) log.error("Error trying to delete file with name : ".concat(filename));
            }

        } catch (IOException e) {
            log.error("An error occurred trying create zip file with name : ".concat(targetName), e);
            throw new FileIOException("An error occurred trying create zip file with name.", e, "SAVExCZIP01");
        }

    }

    /**
     * Iterate through label list, find associated nodes and export them to a CSV file.
     * @throws FileIOException
     */
    private void saveNodes() throws FileIOException {
        while (!openLabelList.isEmpty()) {
            Label toTreat = openLabelList.remove(0);

            String content = "";

            try {
                content = exportLabelToCSV(toTreat);
            } catch (Neo4jNoResult | Neo4jQueryException e) {
                log.error("Error trying to save label : ".concat(toTreat.name()), e);
                MESSAGE_QUEUE.add(new OutputMessage("Error : No nodes found with label : ".concat(toTreat.name())));
                continue;
            }

            String filename = NODE_PREFIX.concat(toTreat.name()).concat(EXTENSION);
            createdFilenameList.add(filename);

            try (FileWriter writer = new FileWriter(pathParams.concat(filename), true)) {
                writer.write(content);
            } catch (Exception e) {
                throw new FileIOException("Error : Impossible to create/open file with name ".concat(filename), e, "SAVExSAVE01");
            }

        }
    }

    public Stream<OutputMessage> save(List<String> labelList,
                                      String path,
                                      String zipFileName,
                                      Boolean saveRelationShip,
                                      Boolean considerNeighbors) throws ProcedureException{
        MESSAGE_QUEUE.clear();

        // Init parameters
        considerNeighborsParams = considerNeighbors;
        saveRelationshipParams = saveRelationShip;
        pathParams = path;

        // Init members
        openLabelList = labelList.stream().map(Label::label).collect(Collectors.toList());

        // openTransaction
        try {
            String targetName = zipFileName.concat(".zip");
            log.info(String.format("Saving Configuration to %s ...", targetName));

            this.tx = db.beginTx();

            saveNodes();
            if(saveRelationshipParams) saveRelationships();

            createZip(targetName);
            MESSAGE_QUEUE.add(new OutputMessage("Saving done"));

            this.tx.commit();

            return MESSAGE_QUEUE.stream();
        } catch (FileIOException e) {
            if(this.tx != null) this.tx.rollback();
            throw new ProcedureException(e);
        } finally {
            if(this.tx != null) this.tx.close();
        }


    }


    public Exporter(GraphDatabaseService db, Log log) {
        this.db = db;
        this.log = log;

        this.closedLabelSet = new HashSet<>();
        this.nodeLabelMap = new HashSet<>();
        this.createdFilenameList = new HashSet<>();
    }
}
