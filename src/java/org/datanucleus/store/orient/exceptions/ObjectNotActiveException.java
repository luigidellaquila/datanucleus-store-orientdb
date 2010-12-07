/**********************************************************************
Copyright (c) 2010 Luigi Dell'Aquila. All rights reserved.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

Contributors:
    ...
**********************************************************************/
package org.datanucleus.store.orient.exceptions;

import org.datanucleus.store.orient.OrientStoreManager;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.StringUtils;

/**
 * 
 */
public class ObjectNotActiveException extends NucleusException
{
    private static final Localiser LOCALISER=Localiser.getInstance("org.datanucleus.store.orient.Localisation",
        OrientStoreManager.class.getClassLoader());

    /** The object that is not activated. */
    private final Object pc;

    /**
     * Constructs a not active exception
     * @param pc The object
     */
    public ObjectNotActiveException(Object pc)
    {
        super(LOCALISER.msg("Orient.ObjectNotActive", StringUtils.toJVMIDString(pc)));
        this.pc = pc;
    }

    /**
     * Accessor for the object that is not active.
     * @return The object
     */
    public Object getObject()
    {
        return pc;
    }
}
