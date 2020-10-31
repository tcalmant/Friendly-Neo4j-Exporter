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


package com.castsoftware.exporter.exceptions.file;

import com.castsoftware.exporter.exceptions.ExporterException;

/**
 * The <code>FilePermissionError</code> is thrown when the procedure can't access a file because it doesn't have the require permissions.
 * FilePermissionError
 */
public class FilePermissionException extends ExporterException {

    private static final long serialVersionUID = -729600314448876926L;
    private static final String messagePrefix = "Error, not enough permission to access the file : ";
    private static final String codePrefix = "FIL_PE_";

    public FilePermissionException(String path, Throwable cause, String code) {
        super(messagePrefix.concat(path), cause, codePrefix.concat(code));
    }

    public FilePermissionException(String message, String path, Throwable cause, String code) {
        super(messagePrefix.concat(message).concat(". Path : ").concat(path), cause, codePrefix.concat(code));
    }
}
