/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.dataone.cn.ldap.v1;

import java.util.ArrayList;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dataone.service.types.v1.Node;
import org.dataone.service.types.v1.NodeList;
import org.dataone.service.types.v1.Service;
import org.dataone.service.util.TypeMarshaller;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.ldap.core.DirContextAdapter;
import org.springframework.ldap.core.DirContextOperations;
import org.springframework.ldap.core.DistinguishedName;
import org.springframework.ldap.core.LdapTemplate;
import org.springframework.stereotype.Component;

/**
 *
 * @author waltz
 */
@Component
@Qualifier("nodeLdapPopulation")
public class NodeLdapPopulation {

    public static List<Node> testNodeList = new ArrayList<Node>();
    public static Log log = LogFactory.getLog(NodeLdapPopulation.class);

    static {
        // Need this or context will lowercase all the rdn s
        System.setProperty(DistinguishedName.KEY_CASE_FOLD_PROPERTY, DistinguishedName.KEY_CASE_FOLD_NONE);
    }
    @Autowired
    @Qualifier("ldapTemplate")
    private LdapTemplate ldapTemplate;
    @Autowired
    @Qualifier("nodeListResource")
    private org.springframework.core.io.Resource nodeListResource;

    public void populateTestMNs() {
        // create a new SystemMetadata object for testing
        NodeList nodeList = null;
        try {
            nodeList = TypeMarshaller.unmarshalTypeFromStream(NodeList.class, nodeListResource.getInputStream());
        } catch (Exception ex) {
            ex.printStackTrace();
            return;
        }
        testNodeList = nodeList.getNodeList();

        for (Node node : testNodeList) {
            DistinguishedName dn = new DistinguishedName();
            dn.add("dc", "dataone");
            dn.add("cn", node.getIdentifier().getValue());
            DirContextAdapter context = new DirContextAdapter(dn);
            mapNodeToContext(node, context);
            ldapTemplate.bind(dn, context, null);
            for (Service service : node.getServices().getServiceList()) {
                String d1NodeServiceId = service.getName() + "-" + service.getVersion();
                DistinguishedName dnService = new DistinguishedName();
                dnService.add("dc", "dataone");
                dnService.add("cn", node.getIdentifier().getValue());
                dnService.add("d1NodeServiceId", d1NodeServiceId);
                context = new DirContextAdapter(dnService);
                mapServiceToContext(service, node.getIdentifier().getValue(), d1NodeServiceId, context);
                ldapTemplate.bind(dnService, context, null);
            }
        }

    }

    protected void mapNodeToContext(Node node, DirContextOperations context) {

        context.setAttributeValue("objectclass", "device");
        context.setAttributeValue("objectclass", "d1Node");
        context.setAttributeValue("cn", node.getIdentifier().getValue());
        context.setAttributeValue("d1NodeId", node.getIdentifier().getValue());
        context.setAttributeValue("d1NodeName", node.getName());
        context.setAttributeValue("d1NodeDescription", node.getDescription());
        context.setAttributeValue("d1NodeBaseURL", node.getBaseURL());
        context.setAttributeValue("d1NodeReplicate", Boolean.toString(node.isReplicate()).toUpperCase());
        context.setAttributeValue("d1NodeSynchronize", Boolean.toString(node.isSynchronize()).toUpperCase());
        context.setAttributeValue("d1NodeType", node.getType().xmlValue());
        context.setAttributeValue("d1NodeState", node.getState().xmlValue());
        context.setAttributeValue("subject", node.getSubject(0).getValue());
        context.setAttributeValue("d1NodeContactSubject", node.getContactSubject(0).getValue());
        context.setAttributeValue("d1NodeApproved", Boolean.toString(Boolean.TRUE).toUpperCase());
    }

    protected void mapServiceToContext(org.dataone.service.types.v1.Service service, String nodeId, String nodeServiceId, DirContextOperations context) {
        context.setAttributeValue("objectclass", "d1NodeService");
        context.setAttributeValue("d1NodeServiceId", nodeServiceId);
        context.setAttributeValue("d1NodeId", nodeId);

        context.setAttributeValue("d1NodeServiceName", service.getName());
        context.setAttributeValue("d1NodeServiceVersion", service.getVersion());
        context.setAttributeValue("d1NodeServiceAvailable", Boolean.toString(service.getAvailable()).toUpperCase());
    }

    public void deletePopulatedMns() {
        for (Node node : testNodeList) {
            if ((node.getServices() != null) && (!node.getServices().getServiceList().isEmpty())) {
                for (Service service : node.getServices().getServiceList()) {
                    deleteNodeService(node, service);
                }
            }
            deleteNode(node);
        }
        testNodeList.clear();
    }

    private void deleteNode(Node node) {
        DistinguishedName dn = new DistinguishedName();
        dn.add("dc", "dataone");
        dn.add("cn", node.getIdentifier().getValue());
        log.info("deleting : " + dn.toString());
        ldapTemplate.unbind(dn);
    }

    private void deleteNodeService(Node node, Service service) {
        String d1NodeServiceId = service.getName() + "-" + service.getVersion();
        DistinguishedName dn = new DistinguishedName();
        dn.add("dc", "dataone");
        dn.add("cn", node.getIdentifier().getValue());
        dn.add("d1NodeServiceId", d1NodeServiceId);
        log.info("deleting : " + dn.toString());
        ldapTemplate.unbind(dn);
    }
}
