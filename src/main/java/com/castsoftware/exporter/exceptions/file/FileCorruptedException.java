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
 * The <code>FileNotFound</code> is thrown when the procedure can't read a file because of corrupted file or bad format.
 * FileNotFound
 */
public class FileCorruptedException extends ExporterException {

    private static final long serialVersionUID = 5538686331898382119L;
    private static final String MESSAGE_PREFIX = "Error, file corrupted and can't be processed by Moirai : ";
    private static final String CODE_PREFIX = "FIL_CR_";


    public FileCorruptedException(String message, String path, Throwable cause, String code) {
        super(MESSAGE_PREFIX.concat(message).concat(". Path : ").concat(path), cause, CODE_PREFIX.concat(code));
    }

    public FileCorruptedException(String message, String code) {
        super(MESSAGE_PREFIX.concat(message), CODE_PREFIX.concat(code));
    }

    public FileCorruptedException(String path, Throwable cause, String code) {
        super(MESSAGE_PREFIX.concat(path), cause, CODE_PREFIX.concat(code));
    }
}
