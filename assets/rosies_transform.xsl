<?xml version="1.0" encoding="UTF-8"?>
<!--
    About this stylesheet:

    This transform is from MODS metadata into an XML structure with Drupal
    field names. This mapping follows, as closely as possible, the
    Islandora Metadata Interest Group (MIG)'s MODS-RDF Simplified Mapping. [1]

    The output xml is designed to be combined by Python (or equivalent) to
    a CSV that can be ingested via Workbench. The reason for the intermediate
    step is that XSLT is less robust than Python at creating properly escaped CSVs.

    An accompanying sheet, metadata-nullifier.xsl, removes the nodes
    (text and attributes) selected by this sheet, and can show you what in
    your data is "missing" from the mapping.

    MODS elements that are not mapped in the MIG mapping are not included
    either in this or in the nullifier and so can be identified and mapped.



    [1] https://docs.google.com/spreadsheets/d/18u2qFJ014IIxlVpM3JXfDEFccwBZcoFsjbBGpvL0jJI/edit#gid=0

-->
<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
    xmlns:xs="http://www.w3.org/2001/XMLSchema"
    xmlns:mods="http://www.loc.gov/mods/v3"
    xmlns:xlink="http://www.w3.org/1999/xlink"
    exclude-result-prefixes="xs mods xlink" version="2.0">

    <xsl:output method="xml" indent="yes"/>

    <!-- MODS -->
    <xsl:template match="mods:mods">
        <row>
            <xsl:apply-templates select="*"/>
            <!-- Add field_pid from filename. -->
            <xsl:variable name="fileName" select="tokenize(base-uri(),'/')[last()]"/>
            <field_pid><xsl:value-of select="replace(replace($fileName, '_MODS.xml',''),'^([^_]*)_','$1:')"/></field_pid>
            <!-- TODO: Add collection from RELS-EXT -->

        </row>
    </xsl:template>

    <!-- ignore non-selected elements -->
    <xsl:template match="text()"/>

    <!-- MODS titleinfo -->
    <!-- Format title - from MODS to DC transform, with (normalize-space() added). -->
    <xsl:template name="titleInfoFormatter">
        <xsl:value-of select="mods:nonSort/normalize-space()"/>
        <xsl:if test="mods:nonSort/normalize-space()">
            <xsl:text> </xsl:text>
        </xsl:if>
        <xsl:value-of select="mods:title/normalize-space()"/>
        <xsl:if test="mods:subTitle/normalize-space()">
            <xsl:text>: </xsl:text>
            <xsl:value-of select="mods:subTitle/normalize-space()"/>
        </xsl:if>
        <xsl:if test="mods:partNumber/normalize-space()">
            <xsl:text>. </xsl:text>
            <xsl:value-of select="mods:partNumber/normalize-space()"/>
        </xsl:if>
        <xsl:if test="mods:partName/normalize-space()">
            <xsl:text>. </xsl:text>
            <xsl:value-of select="mods:partName/normalize-space()"/>
        </xsl:if>
    </xsl:template>

    <xsl:template match="mods:titleInfo[not(@type)][1]">
        <title>
            <xsl:call-template name="titleInfoFormatter"/>
        </title>
    </xsl:template>

    <xsl:template
        match="mods:titleInfo[@type = 'alternative' or @type = 'uniform' or @type = 'abbreviated']">
        <field_alt_title>
            <xsl:call-template name="titleInfoFormatter"/>
        </field_alt_title>
    </xsl:template>

    <xsl:template match="mods:titleInfo[@type = 'translated']">
        <xsl:variable name="titleLang" select="@xml:lang"/>
        <xsl:variable name="translatedTitleField" select="concat('TRANSLATED_TITLE--', $titleLang)"/>
        <xsl:comment>Translated title in [<xsl:value-of select="$titleLang"/>]. Workbench does not have the ability to add multilingual metadata. A translated title is present.</xsl:comment>
        <xsl:element name="{$translatedTitleField}">
            <xsl:call-template name="titleInfoFormatter"/>
        </xsl:element>
    </xsl:template>

    <!-- MODS name -->
    <xsl:template
        match="mods:name[(@type = 'personal' or @type = 'corporate' or @type = 'family') and mods:namePart/text()]">
        <xsl:if test="count(mods:namePart/text()) > 0">
            <field_linked_agent>
                <!-- Name role -->
                <xsl:call-template name="nameRole"/>
                <!-- name type = vocabulary -->
                <xsl:call-template name="nameVocab"/>
                <!-- Name value. Note we are in this section because at least one namePart/text() exists. -->
                <xsl:call-template name="name"/>
            </field_linked_agent>
        </xsl:if>
    </xsl:template>

    <!-- MODS Type of resource (uncontrolled but uppercased) -->
    <xsl:template match="mods:typeOfResource">
        <xsl:comment>TODO: Reconcile resourceType to taxonomy.</xsl:comment>
        <field_resource_type>
            <xsl:value-of select="normalize-space(.)"/>
        </field_resource_type>
        <xsl:if test="@collection = 'yes'">
            <field_resource_type>
                <xsl:text>Collection</xsl:text>
            </field_resource_type>
        </xsl:if>
    </xsl:template>

    <!-- MODS Genre -->
    <xsl:template match="mods:genre">
        <field_genre>
            <xsl:value-of select="normalize-space(.)"/>
        </field_genre>
    </xsl:template>

    <!-- OriginInfo -->
    <xsl:template
        match="mods:originInfo/mods:place/mods:placeTerm[@authority = 'marccountry' and @type = 'code']">
        <field_place_published_country>
            <xsl:variable name="countryCode" select="normalize-space(.)"/>
            <xsl:variable name="countryText"
                select="document('marccountry.xml')/countries/country[@code = $countryCode]/text()"/>
            <xsl:choose>
                <xsl:when test="$countryText">
                    <xsl:value-of select="$countryText"/>
                </xsl:when>
                <xsl:otherwise>
                    <xsl:value-of select="$countryCode"/>
                </xsl:otherwise>
            </xsl:choose>
        </field_place_published_country>
    </xsl:template>
    <xsl:template
        match="mods:originInfo/mods:place/mods:placeTerm[not(@type) or @type = 'text']">
        <field_place_published>
            <xsl:value-of select="normalize-space(.)"/>
        </field_place_published>
    </xsl:template>
    <xsl:template match="mods:originInfo/mods:publisher">
        <field_publisher>
            <xsl:value-of select="normalize-space()"/>
        </field_publisher>
    </xsl:template>
    <xsl:template match="mods:originInfo/mods:dateIssued">
        <field_edtf_date_issued>
            <xsl:value-of select="normalize-space()"/>
        </field_edtf_date_issued>
    </xsl:template>
    <xsl:template match="mods:originInfo/mods:dateCreated">
        <field_edtf_date_created>
            <xsl:value-of select="normalize-space()"/>
        </field_edtf_date_created>
    </xsl:template>
    <xsl:template match="mods:originInfo/mods:dateValid">
        <field_date_valid>
            <xsl:value-of select="normalize-space()"/>
        </field_date_valid>
    </xsl:template>
    <xsl:template match="mods:originInfo/mods:dateCaptured">
        <field_date_captured>
            <xsl:value-of select="normalize-space()"/>
        </field_date_captured>
    </xsl:template>
    <xsl:template match="mods:originInfo/mods:dateModified">
        <field_date_modified>
            <xsl:value-of select="normalize-space()"/>
        </field_date_modified>
    </xsl:template>
    <xsl:template match="mods:originInfo/mods:copyrightDate">
        <field_copyright_date>
            <xsl:value-of select="normalize-space()"/>
        </field_copyright_date>
    </xsl:template>
    <xsl:template match="mods:originInfo/mods:dateOther">
        <field_edtf_date>
            <xsl:value-of select="normalize-space()"/>
        </field_edtf_date>
    </xsl:template>

    <xsl:template match="mods:originInfo/mods:frequency">
        <field_frequency>
            <xsl:value-of select="normalize-space()"/>
        </field_frequency>
    </xsl:template>

    <xsl:template match="mods:originInfo/mods:issuance">
        <field_mode_of_issuance>
            <xsl:value-of select="normalize-space()"/>
        </field_mode_of_issuance>
    </xsl:template>

    <xsl:template match="mods:originInfo/mods:edition">
        <field_edition>
            <xsl:value-of select="normalize-space()"/>
        </field_edition>
    </xsl:template>


    <!-- MDOS Language -->
    <xsl:template match="mods:language">
        <field_language>
            <xsl:choose>
                <xsl:when test="mods:languageTerm[@type = 'text']">
                    <xsl:value-of select="mods:languageTerm[@type = 'text']"/>
                </xsl:when>
                <xsl:when test="mods:languageTerm[@authority = 'iso639-2b' and @type = 'code']">
                    <xsl:variable name="langCode"
                        select="mods:languageTerm[@authority = 'iso639-2b' and @type = 'code']/text()"/>
                    <xsl:variable name="langText"
                        select="document('ISO-639-2.xml')/languages/language[@code = $langCode]/text()"/>
                    <xsl:choose>
                        <xsl:when test="$langText">
                            <xsl:value-of select="$langText"/>
                        </xsl:when>
                        <xsl:otherwise>
                            <xsl:comment>DATA ERROR: Language code could not be found in ISO-639-2b list.</xsl:comment>
                            <xsl:value-of select="$langCode"/>
                        </xsl:otherwise>
                    </xsl:choose>
                </xsl:when>
            </xsl:choose>
        </field_language>
    </xsl:template>


    <!-- MODS physical description -->
    <xsl:template match="mods:physicalDescription/mods:form">
        <field_physical_form>
            <xsl:value-of select="text()"/>
        </field_physical_form>
        <xsl:comment>TODO: map form to taxonomy.</xsl:comment>
    </xsl:template>

    <xsl:template match="mods:physicalDescription/mods:extent">
        <field_extent>
            <xsl:value-of select="text()/normalize-space()"/>
        </field_extent>
    </xsl:template>

    <xsl:template match="mods:physicalDescription/mods:internetMediaType">
        <xsl:comment>Media type is a property of the media and will be calculated programmatically.</xsl:comment>
        <MIMETYPE>
            <xsl:value-of select="text()/normalize-space()"/>
        </MIMETYPE>
    </xsl:template>

    <!-- MODS abstract -->
    <xsl:template match="mods:abstract">
        <field_abstract>
            <xsl:value-of select="text()/normalize-space()"/>
        </field_abstract>
    </xsl:template>

    <!-- MODS table of contents -->
    <xsl:template match="mods:tableOfContents">
        <field_table_of_contents>
            <xsl:value-of select="text()/normalize-space()"/>
        </field_table_of_contents>
    </xsl:template>

    <!-- MODS note -->
    <xsl:template match="mods:note">
        <field_note>
            <xsl:variable name="noteContent" select="normalize-space(.)"/>
            <xsl:choose>
                <xsl:when test="@displayLabel">
                    <xsl:variable name="notePrefix" select="concat(@displayLabel, ': ')"/>
                    <xsl:value-of select="concat($notePrefix, $noteContent)"/>
                </xsl:when>
                <xsl:when test="@type">
                    <xsl:variable name="notePrefix"
                        select="concat(upper-case(substring(@type, 1, 1)), substring(@type, 2), ': ')"/>
                    <xsl:value-of select="concat($notePrefix, $noteContent)"/>
                </xsl:when>
                <xsl:otherwise>
                    <xsl:value-of select="$noteContent"/>
                </xsl:otherwise>
            </xsl:choose>
        </field_note>
    </xsl:template>

    <!-- MODS Subject -->
    <xsl:template match="mods:subject">
        <xsl:call-template name="subject"/>
    </xsl:template>

    <!-- MODS Classification-->
    <xsl:template match="mods:classification">
        <xsl:choose>
            <xsl:when test="@authority = 'lcc'">
                <field_lcc_classification>
                    <xsl:value-of select="text()"/>
                </field_lcc_classification>
            </xsl:when>
            <xsl:when test="@authority = 'ddc'">
                <field_dewey_classification>
                    <xsl:value-of select="text()"/>
                </field_dewey_classification>
            </xsl:when>
            <xsl:otherwise>
                <field_classification>
                    <xsl:value-of select="text()"/>
                </field_classification>
            </xsl:otherwise>
        </xsl:choose>
    </xsl:template>

    <!-- MODS Identifier -->
    <xsl:template match="mods:identifier">
        <xsl:choose>
            <xsl:when test="@type = 'isbn'">
                <field_isbn>
                    <xsl:value-of select="text()"/>
                </field_isbn>
            </xsl:when>
            <xsl:when test="@type = 'oclc'">
                <field_oclc_number>
                    <xsl:value-of select="text()"/>
                </field_oclc_number>
            </xsl:when>
            <xsl:when test="@type = 'local'">
                <field_local_identifier>
                    <xsl:value-of select="text()"/>
                </field_local_identifier>
            </xsl:when>
            <xsl:otherwise>
                <field_identifier>
                    <xsl:value-of select="text()"/>
                </field_identifier>
            </xsl:otherwise>
        </xsl:choose>
    </xsl:template>

    <!-- MODS AccessCondition -->
    <xsl:template match="mods:accessCondition">
        <field_rights>
            <xsl:value-of select="text()/normalize-space()"/>
        </field_rights>
    </xsl:template>

    <xsl:template name="name-dc">
        <xsl:variable name="name">
            <xsl:for-each select="mods:namePart[not(@type)]">
                <!-- CUSTOM: modified to not add extra spaces. -->
                <xsl:if test="position() != 1"> </xsl:if>
                <xsl:value-of select="."/>

            </xsl:for-each>
            <xsl:value-of select="mods:namePart[@type = 'family']"/>
            <xsl:if test="mods:namePart[@type = 'given']">
                <xsl:text>, </xsl:text>
                <xsl:value-of select="mods:namePart[@type = 'given']"/>
            </xsl:if>
            <xsl:if test="mods:namePart[@type = 'date']">
                <xsl:text>, </xsl:text>
                <xsl:value-of select="mods:namePart[@type = 'date']"/>
                <xsl:text/>
            </xsl:if>
        </xsl:variable>
        <xsl:value-of select="normalize-space($name)"/>
    </xsl:template>

    <xsl:template name="name">
        <xsl:choose>
            <!-- single namePart -->
            <xsl:when test="count(mods:namePart) = 1">
                <xsl:value-of select="mods:namePart/text()"/>
            </xsl:when>
            <!-- multiple nameParts, all with types => implode in LCNAF order with comma space. -->
            <xsl:when test="count(mods:namePart[not(@type)]) = 0">
                <xsl:variable name="given" select="mods:namePart[@type = 'given']/text()"/>
                <xsl:variable name="family" select="mods:namePart[@type = 'family']/text()"/>
                <xsl:variable name="date" select="mods:namePart[@type = 'date']/text()"/>
                <xsl:variable name="termsOfAddress"
                    select="mods:namePart[@type = 'termsOfAddress']/text()"/>
                <xsl:value-of select="string-join(($family, $given, $termsOfAddress, $date), ', ')"
                />
            </xsl:when>
            <!-- otherwise => implode in natural order with comma space -->
            <xsl:otherwise>
                <xsl:for-each select="mods:namePart">
                    <xsl:value-of select="."/>
                    <xsl:if test="position() != last()">, </xsl:if>
                </xsl:for-each>
            </xsl:otherwise>
        </xsl:choose>
    </xsl:template>

    <xsl:template name="nameVocab">
        <xsl:variable name="nameType" select="@type"/>
        <xsl:if test="$nameType = 'corporate'">
            <xsl:variable name="nameType"
                select="replace($nameType, 'corporate', 'corporate_body')"/>
            <xsl:value-of select="$nameType"/>
            <xsl:text>:</xsl:text>
        </xsl:if>
        <xsl:if test="$nameType = 'personal'">
            <xsl:variable name="nameType" select="replace($nameType, 'personal', 'person')"/>
            <xsl:value-of select="$nameType"/>
            <xsl:text>:</xsl:text>
        </xsl:if>
        <xsl:if test="$nameType = 'family'">
            <xsl:value-of select="$nameType"/>
            <xsl:text>:</xsl:text>
        </xsl:if>
    </xsl:template>

    <xsl:template name="nameRole">
        <!--relator term, defaults to relators:ctb per mapping -->
        <xsl:choose>
            <xsl:when
                test="mods:role/mods:roleTerm[@authority = 'marcrelator' and @type = 'code']">
                <xsl:value-of
                    select="mods:role/mods:roleTerm[@authority = 'marcrelator' and @type = 'code']/text()"/>
                <xsl:text>:</xsl:text>
            </xsl:when>
            <xsl:when
                test="mods:role/mods:roleTerm[@authority = 'marcrelator' and @type = 'text']">
                <xsl:variable name="roleText"
                    select="mods:role/mods:roleTerm[@authority = 'marcrelator' and @type = 'text']/text()"/>
                <xsl:variable name="roleText"
                    select="concat(upper-case(substring($roleText, 1, 1)), substring($roleText, 2))"/>
                <xsl:variable name="roleCode"
                    select="document('relators.xml')/relators/relator[text() = $roleText]/@code"/>
                <xsl:choose>
                    <xsl:when test="$roleCode">
                        <xsl:value-of select="$roleCode"/>
                    </xsl:when>
                    <xsl:otherwise>
                        <xsl:comment>DATA ERROR: Relator text could not be found in code list.</xsl:comment>
                        <xsl:value-of select="$roleText"/>
                    </xsl:otherwise>
                </xsl:choose>
                <xsl:text>:</xsl:text>
            </xsl:when>
            <xsl:otherwise>
                <xsl:text>relators:ctb</xsl:text>
                <xsl:text>:</xsl:text>
            </xsl:otherwise>
        </xsl:choose>
    </xsl:template>

    <xsl:template name="subject">
        <!-- MIG mapping indicates breaking up composed headings. See LC's MODS to DC for an alternative.-->
        <xsl:for-each select="mods:topic">
            <field_subject>
                <xsl:value-of select="normalize-space(.)"/>
            </field_subject>
        </xsl:for-each>
        <xsl:for-each select="mods:name">
            <field_subjects_name>
                <xsl:call-template name="nameVocab"/>
                <xsl:call-template name="name-dc"/>
            </field_subjects_name>
        </xsl:for-each>
        <xsl:for-each select="mods:geographic">
            <field_geographic_subject>
                <xsl:value-of select="normalize-space(.)"/>
            </field_geographic_subject>
        </xsl:for-each>
        <!-- note: hierarchicalGeographic remains "composed": Canada[double dash]Prince Edward Island[double dash]Charlottetown. -->
        <xsl:for-each select="mods:hierarchicalGeographic">
            <field_geographic_subject>
                <xsl:for-each
                    select="mods:continent | mods:country | mods:province | mods:region | mods:state | mods:territory | mods:county | mods:city | mods:citySection | mods:island | mods:area">
                    <xsl:value-of select="."/>
                    <xsl:if test="(text() and position() != last())">--</xsl:if>
                </xsl:for-each>
            </field_geographic_subject>
        </xsl:for-each>
        <!-- MIG mapping excludes all cartographic elements except coordinates. -->
        <xsl:for-each select="mods:cartographics/mods:coordinates">
            <field_coordinates>
                <xsl:value-of select="normalize-space(.)"/>
            </field_coordinates>
        </xsl:for-each>
        <xsl:if test="mods:temporal">
            <field_temporal_subject>
                <xsl:for-each select="mods:temporal">
                    <xsl:value-of select="normalize-space(.)"/>
                    <xsl:if test="(text() and position() != last())">-</xsl:if>
                </xsl:for-each>
            </field_temporal_subject>
        </xsl:if>
        <xsl:for-each select="mods:genre">
            <field_genre>
                <xsl:value-of select="normalize-space(.)"/>
            </field_genre>
        </xsl:for-each>
    </xsl:template>

    <xsl:template match="mods:relatedItem">
        <xsl:variable name="type" select="@type"/>
        <xsl:variable name="href" select="@xlink:href"/>
        <xsl:variable name="filename" select="tokenize(base-uri(),'/')[last()]" />
        <xsl:variable name="index" select="position()"/>
        <xsl:variable name="relfilename" select="concat($filename, '-related-', $type, '-', $index, '-', replace($href, ':', '_'), '.xml')"/>
        <xsl:result-document method="xml" href="{$relfilename}">
            <row>
                <xsl:apply-templates select="*"/>
            </row>
        </xsl:result-document>
    </xsl:template>

</xsl:stylesheet>
