/**
 * Copyright 2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import models.LineagePathInfo;
import org.apache.commons.lang3.StringUtils;

import play.Logger;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class Lineage
{

    public static final String URNIDMAPKey = "impactUrnIDMap";

    public static LineagePathInfo convertFromURN(String urn)
    {
        if (StringUtils.isBlank(urn))
            return null;

        LineagePathInfo pathInfo = new LineagePathInfo();

        String[] pathArray = urn.split(":///");
        if (pathArray != null && pathArray.length > 0)
        {
            String storageType = pathArray[0];
            pathInfo.storageType = storageType;
            if (StringUtils.isNotBlank(storageType))
            {
                if (storageType.equalsIgnoreCase("hdfs"))
                {
                    if (pathArray.length > 1 && StringUtils.isNotBlank(pathArray[1]))
                    {
                        pathInfo.filePath = "/" + pathArray[1];
                    }
                }
                else if (storageType.equalsIgnoreCase("teradata"))
                {
                    if (pathArray.length > 1 && StringUtils.isNotBlank(pathArray[1]))
                    {
                        int index = pathArray[1].indexOf("/");
                        if (index != -1)
                        {
                            pathInfo.schemaName = pathArray[1].substring(0, index);
                            pathInfo.filePath = pathArray[1].substring(index+1);
                        }
                    }
                }
                else if (storageType.equalsIgnoreCase("nas"))
                {
                    if (pathArray.length > 1 && StringUtils.isNotBlank(pathArray[1]))
                    {
                        pathInfo.filePath = "/" + pathArray[1];
                    }
                }
                else if (storageType.equalsIgnoreCase("hive"))
                {
                  if (pathArray.length > 1 && StringUtils.isNotBlank(pathArray[1]))
                  {
                      pathInfo.filePath = "/" + pathArray[1];
                  }
                }
                else
                {
                    pathInfo.storageType = null;
                    pathInfo.schemaName = null;
                    pathInfo.filePath = urn;
                }
            }
            else
            {
                pathInfo.storageType = null;
                pathInfo.schemaName = null;
                pathInfo.filePath = urn;
            }
        }

        return pathInfo;
    }

    public static String convertToURN(LineagePathInfo pathInfo)
    {
        if (pathInfo == null)
            return null;

        String filePath = "";
        if (StringUtils.isNotBlank(pathInfo.filePath))
        {
            if(pathInfo.filePath.charAt(0) == '/')
            {
                filePath = pathInfo.filePath.substring(1);
            }
            else
            {
                filePath = pathInfo.filePath;
            }
        }
        if (StringUtils.isNotBlank(pathInfo.storageType) && pathInfo.storageType.equalsIgnoreCase("teradata"))
        {
            return "teradata:///" + pathInfo.schemaName + "/" + filePath;
        }
        else
        {
            return pathInfo.storageType.toLowerCase() + ":///" + filePath;
        }
    }
}
