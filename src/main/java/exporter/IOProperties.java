/*
 *  Copyright (C) 2020  Hugo JOBY
 *
 *     This program is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU General Public License as published by
 *     the Free Software Foundation, either version 3 of the License, or
 *     (at your option) any later version.
 *
 *     This program is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU General Public License for more details.
 *
 *     You should have received a copy of the GNU General Public License
 *     along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package exporter;

import exceptions.ExporterException;
import exceptions.file.FileCorruptedException;
import exceptions.file.FileIOException;

import java.io.IOException;
import java.util.Properties;

public class IOProperties {

    public static final String CONFIGURATION_PATH = "/io.properties";

    public enum Property {
        CSV_EXTENSION("io.csv.csv_extension"),
        CSV_DELIMITER("io.csv.delimiter"),
        INDEX_COL("io.index_col"),
        INDEX_OUTGOING("io.index_outgoing"),
        INDEX_INCOMING("io.index_incoming"),
        PREFIX_NODE_FILE("io.file.prefix.node"),
        PREFIX_RELATIONSHIP_FILE("io.file.prefix.relationship");

        private String label;

        @Override
        public String toString() {
            try {
                return loadFromPropertyFile(this.label);
            } catch (ExporterException e) {
                e.printStackTrace();
                return null;
            }
        }

        Property(String label) {
            this.label = label;
        }
    }

    private static Properties properties;

    //Config node name
    public static  String loadFromPropertyFile(String key) throws FileIOException, FileCorruptedException {
        if (properties == null) {
            properties = new Properties();
            try {
                properties.load(IOProperties.class.getResourceAsStream(CONFIGURATION_PATH));
            } catch (IOException | NullPointerException e) {
                throw new FileIOException("Cannot read config.properties file", CONFIGURATION_PATH, e, "CONFxLPR1");
            } catch (IllegalArgumentException e) {
                throw new FileCorruptedException("Corrupted config.properties file", CONFIGURATION_PATH, e, "CONFxLPR2");
            }
        }
        try {
            return  (String) properties.get(key);
        } catch (NullPointerException e) {
            throw new FileCorruptedException("Cannot read properties in config.properties file", CONFIGURATION_PATH, e, "CONFxLPR1");
        }
    }

    public IOProperties() {
    }
}
