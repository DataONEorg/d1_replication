/**
 * This work was created by participants in the DataONE project, and is
 * jointly copyrighted by participating institutions in DataONE. For 
 * more information on DataONE, see our web site at http://dataone.org.
 *
 *   Copyright ${year}
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and 
 * limitations under the License.
 * 
 * $Id$
 */

package org.dataone.cn.ldap.v2;

import javax.naming.NamingException;

import org.dataone.configuration.Settings;
import org.springframework.ldap.core.support.DefaultDirObjectFactory;
import org.springframework.ldap.core.support.LdapContextSource;

/**
 *
 * @author waltz
 */
public class ContextSourceConfiguration {

    LdapContextSource ldapContextSource;

    // look up defaults from configuration
    private String server = Settings.getConfiguration().getString("cn.ldap.server");
    private String admin = Settings.getConfiguration().getString("cn.ldap.admin");
    private String password = Settings.getConfiguration().getString("cn.ldap.password");
    private String base = Settings.getConfiguration().getString("nodeRegistry.ldap.base");

    public ContextSourceConfiguration() throws NamingException {
        ldapContextSource = new LdapContextSource();
        ldapContextSource.setDirObjectFactory(DefaultDirObjectFactory.class);
        ldapContextSource.setUrl(server);
        ldapContextSource.setBase(base);
        ldapContextSource.setUserDn(admin);
        ldapContextSource.setPassword(password);
    }

    public LdapContextSource getLdapContextSource() {
        return ldapContextSource;
    }
}
