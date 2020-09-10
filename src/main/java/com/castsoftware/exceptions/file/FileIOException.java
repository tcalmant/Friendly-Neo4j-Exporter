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


package com.castsoftware.exceptions.file;
import com.castsoftware.exceptions.ExporterException;

/**
 * The <code>FileNotFound</code> is thrown when the procedure can't access a file because it doesn't exist, or the path resolution failed.
 * FileNotFound
 */
public class FileIOException extends ExporterException {

    private static final long serialVersionUID = -622271594516405222L;
    private static final String messagePrefix = "Error, IO exception during file operation : ";
    private static final String codePrefix = "FIL_IO_";

    public FileIOException(String message, String path, Throwable cause, String code) {
        super(messagePrefix.concat(message).concat(". Path : ").concat(path), cause, codePrefix.concat(code));
    }

    public FileIOException(String path, Throwable cause, String code) {
        super(messagePrefix.concat(path), cause, codePrefix.concat(code));
    }
}
