#!/usr/bin/env bash
java -cp ".:hadoopl-1.0.jar" yarn.app.distributedshell.Client \
-jar hadoopl-1.0.jar \
-shell_script task.sh \
-log_properties log4j.properties
