package org.datanucleus.store.orient.exceptions;

import org.datanucleus.exceptions.NucleusException;

public class NotSupportedMethodException extends NucleusException
{
    /**The message to print*/
    final static String msg = "NotSupportedMethodException";

    /**The affected methods*/
    final static String methods = "indexOf, substring, toLowerCase, toUpperCase, " +
    		"charAt, length, equals, isEmpty, containsKey, containsValue, containsEntry";

    /**
     * Constructor for Execption if an unsupported Method is connected with OR.
     */
    public NotSupportedMethodException()
    {
        super(msg);
    }
}
