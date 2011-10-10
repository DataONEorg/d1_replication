====
    This work was created by participants in the DataONE project, and is
    jointly copyrighted by participating institutions in DataONE. For
    more information on DataONE, see our web site at http://dataone.org.

      Copyright ${year}

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.

    $Id$
====

DataONE Replication Component
---------------------------

d1_replication is a small set of java classes that manage Member Node to 
Member Node replication of data and metadata objects by monitoring changes
in the DataONE system. This is a component of the Coordinating Node software stack,
and is instantiated by the d1_process_daemon web application.  When additions
or changes to objects are registered on the Coordinating Nodes, the 
ReplicationManager class evaluates the ReplicationPolicy section of the 
SystemMetadata for the given object, along with the Node capabilities for
potential replica Member Nodes, and builds replication tasks that are executed
on a Coordinating Node.  These replication tasks initiate replication by
calling MNReplication.replicate() on the target Member Node.

Installation
------------
Use 'mvn clean install' to install the jar file locally.  The d1_process_daemon
component depends on d1_replication and includes this jar in its deployed
package.

See LICENSE.txt for the details of distributing this software. 
