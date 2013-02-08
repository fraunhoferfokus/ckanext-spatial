# -*- coding: utf-8 -*-
from lxml import etree
from HTMLParser import HTMLParser
    
import logging
log = logging.getLogger(__name__)


class MappedXmlObject(object):
    elements = []


class MappedXmlDocument(MappedXmlObject):
    def __init__(self, xml_str=None, xml_tree=None):
        assert (xml_str or xml_tree is not None), 'Must provide some XML in one format or another'
        self.xml_str = xml_str
        self.xml_tree = xml_tree

    def read_values(self):
        '''For all of the elements listed, finds the values of them in the
XML and returns them.'''
        values = {}
        tree = self.get_xml_tree()
        for element in self.elements:
            values[element.name] = element.read_value(tree)

            # unescape HTML/XML references
            values[element.name] = HTMLParser().unescape(values[element.name])
            
        self.infer_values(values)
        return values

    def read_value(self, name):
        '''For the given element name, find the value in the XML and return
it.
'''
        tree = self.get_xml_tree()
        for element in self.elements:
            if element.name == name:
                return element.read_value(tree)
        raise KeyError

    def get_xml_tree(self):
        if self.xml_tree is None:
            parser = etree.XMLParser(remove_blank_text=True)
            if type(self.xml_str) == unicode:
                xml_str = self.xml_str.encode('utf8')
            else:
                xml_str = self.xml_str
            self.xml_tree = etree.fromstring(xml_str, parser=parser)
        return self.xml_tree

    def infer_values(self, values):
        pass


class MappedXmlElement(MappedXmlObject):
    namespaces = {}

    def __init__(self, name, search_paths=[], multiplicity="*", elements=[]):
        self.name = name
        self.search_paths = search_paths
        self.multiplicity = multiplicity
        self.elements = elements or self.elements

    def read_value(self, tree):
        values = []
        for xpath in self.get_search_paths():
            elements = self.get_elements(tree, xpath)
            values = self.get_values(elements)
            if values:
                break
        return self.fix_multiplicity(values)

    def get_search_paths(self):
        if type(self.search_paths) != type([]):
            search_paths = [self.search_paths]
        else:
            search_paths = self.search_paths
        return search_paths

    def get_elements(self, tree, xpath):
        return tree.xpath(xpath, namespaces=self.namespaces)

    def get_values(self, elements):
        values = []
        if len(elements) == 0:
            pass
        else:
            for element in elements:
                value = self.get_value(element)
                values.append(value)
        return values

    def get_value(self, element):
        if self.elements:
            value = {}
            for child in self.elements:
                value[child.name] = child.read_value(element)
            return value
        elif type(element) == etree._ElementStringResult:
            value = str(element)
        elif type(element) == etree._ElementUnicodeResult:
            value = unicode(element)
        else:
            value = self.element_tostring(element)
        return value

    def element_tostring(self, element):
        return etree.tostring(element, pretty_print=False)

    def fix_multiplicity(self, values):
        if self.multiplicity == "0":
            if values:
                raise Exception(
                    "Values found for element '%s': %s" % (self.name, values))
            else:
                return ""
        elif self.multiplicity == "1":
            if values:
                return values[0]
            else:
                raise Exception(
                    "Value not found for element '%s'" % self.name)
        elif self.multiplicity == "*":
            return values
        elif self.multiplicity == "0..1":
            if values:
                return values[0]
            else:
                return ""
        elif self.multiplicity == "1..*":
            return values
        else:
            raise Exception(
                "Can't fix element values for multiplicity '%s'." % \
                                self.multiplicity)


class GeminiElement(MappedXmlElement):

    namespaces = {
       "gts": "http://www.isotc211.org/2005/gts",
       "gml": "http://www.opengis.net/gml",
       "gmx": "http://www.isotc211.org/2005/gmx",
       "gsr": "http://www.isotc211.org/2005/gsr",
       "gss": "http://www.isotc211.org/2005/gss",
       "gco": "http://www.isotc211.org/2005/gco",
       "gmd": "http://www.isotc211.org/2005/gmd",
       "srv": "http://www.isotc211.org/2005/srv",
       "xlink": "http://www.w3.org/1999/xlink",
       "xsi": "http://www.w3.org/2001/XMLSchema-instance",
    }


class GeminiResponsibleParty(GeminiElement):

    elements = [
        GeminiElement(
            name="organisation-name",
            search_paths=[
                "gmd:organisationName/gco:CharacterString/text()",
            ],
            multiplicity="0..1",
        ),
        GeminiElement(
            name="individual-name",
            search_paths=[
                "gmd:individualName/gco:CharacterString/text()",
            ],
            multiplicity="0..1",
        ),
        GeminiElement(
            name="position-name",
            search_paths=[
                "gmd:positionName/gco:CharacterString/text()",
            ],
            multiplicity="0..1",
        ),
        GeminiElement(
            name="contact-info",
            search_paths=[
                "gmd:contactInfo/gmd:CI_Contact",
            ],
            multiplicity="0..1",
            elements=[
                GeminiElement(
                    name="email",
                    search_paths=[
                        "gmd:address/gmd:CI_Address/gmd:electronicMailAddress/gco:CharacterString/text()",
                    ],
                    multiplicity="0..1",
                ),
                GeminiElement(
                    name="deliveryPoint",
                    search_paths=[
                        "gmd:address/gmd:CI_Address/gmd:deliveryPoint/gco:CharacterString/text()",
                    ],
                    multiplicity="0..1",
                ),
                GeminiElement(
                    name="city",
                    search_paths=[
                        "gmd:address/gmd:CI_Address/gmd:city/gco:CharacterString/text()",
                    ],
                    multiplicity="0..1",
                ),
                GeminiElement(
                    name="adminitrativeArea",
                    search_paths=[
                        "gmd:address/gmd:CI_Address/gmd:adminitrativeArea/gco:CharacterString/text()",
                    ],
                    multiplicity="0..1",
                ),
                GeminiElement(
                    name="postalCode",
                    search_paths=[
                        "gmd:address/gmd:CI_Address/gmd:postalCode/gco:CharacterString/text()",
                    ],
                    multiplicity="0..1",
                ),
                GeminiElement(
                    name="country",
                    search_paths=[
                        "gmd:address/gmd:CI_Address/gmd:country/gco:CharacterString/text()",
                    ],
                    multiplicity="0..1",
                ),
                GeminiElement(
                    name="url",
                    search_paths=[
                        "gmd:onlineResource/gmd:CI_OnlineResource/gmd:linkage/gmd:URL/text()",
                    ],
                    multiplicity="0..1",
                ),
            ]
        ),
        GeminiElement(
            name="role",
            search_paths=[
                "gmd:role/gmd:CI_RoleCode/@codeListValue",
            ],
            multiplicity="0..1",
        ),
    ]


class GeminiResourceLocator(GeminiElement):

    elements = [
        GeminiElement(
            name="url",
            search_paths=[
                "gmd:linkage/gmd:URL/text()",
            ],
            multiplicity="1",
        ),
        GeminiElement(
            name="function",
            search_paths=[
                "gmd:function/gmd:CI_OnLineFunctionCode/@codeListValue",
            ],
            multiplicity="0..1",
        ),
        GeminiElement(
            name="name",
            search_paths=[
                "gmd:name/gco:CharacterString/text()",
            ],
            multiplicity="0..1",
        ),
        GeminiElement(
            name="description",
            search_paths=[
                "gmd:description/gco:CharacterString/text()",
            ],
            multiplicity="0..1",
        ),
        GeminiElement(
            name="protocol",
            search_paths=[
                "gmd:protocol/gco:CharacterString/text()",
            ],
            multiplicity="0..1",
        ),
        ]


class GeminiDataFormat(GeminiElement):

    elements = [
        GeminiElement(
            name="name",
            search_paths=[
                "gmd:name/gco:CharacterString/text()",
            ],
            multiplicity="0..1",
        ),
        GeminiElement(
            name="version",
            search_paths=[
                "gmd:version/gco:CharacterString/text()",
            ],
            multiplicity="0..1",
        ),
    ]



class GeminiReferenceDate(GeminiElement):

    elements = [
        GeminiElement(
            name="type",
            search_paths=[
                "gmd:dateType/gmd:CI_DateTypeCode/@codeListValue",
                "gmd:dateType/gmd:CI_DateTypeCode/text()",
            ],
            multiplicity="0..1",
        ),
        GeminiElement(
            name="value",
            search_paths=[
                "gmd:date/gco:Date/text()",
                "gmd:date/gco:DateTime/text()",
            ],
            # TODO: check
            multiplicity="*",
        ),
    ]

class GeminiCoupledResources(GeminiElement):

    elements = [
        GeminiElement(
            name="title",
            search_paths=[
                "@xlink:title",
            ],
            multiplicity="*",
        ),
        GeminiElement(
            name="href",
            search_paths=[
                "@xlink:href",
            ],
            multiplicity="*",
        ),
        GeminiElement(
            name="uuid",
            search_paths=[
                "@uuidref",
            ],
            multiplicity="*",
        ),

    ]

class GeminiDocument(MappedXmlDocument):

    # Attribute specifications from "XPaths for GEMINI" by Peter Parslow.

    elements = [
        GeminiElement(
            name="guid",
            search_paths="gmd:fileIdentifier/gco:CharacterString/text()",
            multiplicity="0..1",
        ),
        GeminiElement(
            name="metadata-language",
            search_paths=[
                "gmd:language/gmd:LanguageCode/@codeListValue",
                "gmd:language/gmd:LanguageCode/text()",
            ],
            multiplicity="0..1",
        ),
        GeminiElement(
            name="resource-type",
            search_paths=[
                "gmd:hierarchyLevel/gmd:MD_ScopeCode/@codeListValue",
                "gmd:hierarchyLevel/gmd:MD_ScopeCode/text()",
            ],
            multiplicity="0..1*",
        ),
        GeminiResponsibleParty(
            name="metadata-point-of-contact",
            search_paths=[
                "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:pointOfContact/gmd:CI_ResponsibleParty",
                "gmd:identificationInfo/srv:SV_ServiceIdentification/gmd:pointOfContact/gmd:CI_ResponsibleParty",
            ],
            multiplicity="1..*",
        ),
        GeminiElement(
            name="metadata-date",
            search_paths=[
                "gmd:dateStamp/gco:DateTime/text()",
                "gmd:dateStamp/gco:Date/text()",
            ],
            multiplicity="1",
        ),
        GeminiElement(
            name="spatial-reference-system",
            search_paths=[
                "gmd:referenceSystemInfo/gmd:MD_ReferenceSystem/gmd:referenceSystemIdentifier/gmd:RS_Identifier/gmd:code/gco:CharacterString/text()",
            ],
            multiplicity="0..1",
        ),
        GeminiElement(
            name="title",
            search_paths=[
                "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:citation/gmd:CI_Citation/gmd:title/gco:CharacterString/text()",
                "gmd:identificationInfo/srv:SV_ServiceIdentification/gmd:citation/gmd:CI_Citation/gmd:title/gco:CharacterString/text()",
            ],
            multiplicity="1",
        ),
        GeminiElement(
            name="alternative-title",
            search_paths=[
                "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:citation/gmd:CI_Citation/gmd:alternativeTitle/gco:CharacterString/text()",
                "gmd:identificationInfo/srv:SV_ServiceIdentification/gmd:citation/gmd:CI_Citation/gmd:alternativeTitle/gco:CharacterString/text()",
            ],
            multiplicity="*",
        ),
        GeminiReferenceDate(
            name="dataset-reference-date",
            search_paths=[
                "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:citation/gmd:CI_Citation/gmd:date/gmd:CI_Date",
                "gmd:identificationInfo/srv:SV_ServiceIdentification/gmd:citation/gmd:CI_Citation/gmd:date/gmd:CI_Date",
            ],
            multiplicity="*",
        ),
        # # Todo: Suggestion from PP not to bother pulling this into the package.
        # GeminiElement(
        # name="unique-resource-identifier",
        # search_paths=[
        # "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:citation/gmd:CI_Citation/gmd:identifier/gmd:RS_Identifier",
        # "gmd:identificationInfo/srv:SV_ServiceIdentification/gmd:citation/gmd:CI_Citation/gmd:identifier/gmd:RS_Identifier",
        # ],
        # multiplicity="1",
        # ),
        GeminiElement(
            name="abstract",
            search_paths=[
                "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:abstract/gco:CharacterString/text()",
                "gmd:identificationInfo/srv:SV_ServiceIdentification/gmd:abstract/gco:CharacterString/text()",
            ],
            multiplicity="1",
        ),
        GeminiResponsibleParty(
            name="responsible-organisation",
            search_paths=[
                "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:pointOfContact/gmd:CI_ResponsibleParty",
                "gmd:identificationInfo/srv:SV_ServiceIdentification/gmd:pointOfContact/gmd:CI_ResponsibleParty",
                "gmd:contact/gmd:CI_ResponsibleParty",
            ],
            multiplicity="1..*",
        ),
        GeminiElement(
            name="frequency-of-update",
            search_paths=[
                "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:resourceMaintenance/gmd:MD_MaintenanceInformation/gmd:maintenanceAndUpdateFrequency/gmd:MD_MaintenanceFrequencyCode/@codeListValue",
                "gmd:identificationInfo/srv:SV_ServiceIdentification/gmd:resourceMaintenance/gmd:MD_MaintenanceInformation/gmd:maintenanceAndUpdateFrequency/gmd:MD_MaintenanceFrequencyCode/@codeListValue",

                "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:resourceMaintenance/gmd:MD_MaintenanceInformation/gmd:maintenanceAndUpdateFrequency/gmd:MD_MaintenanceFrequencyCode/text()",
                "gmd:identificationInfo/srv:SV_ServiceIdentification/gmd:resourceMaintenance/gmd:MD_MaintenanceInformation/gmd:maintenanceAndUpdateFrequency/gmd:MD_MaintenanceFrequencyCode/text()",
            ],
            multiplicity="0..1",
        ),
        GeminiElement(
            name="keyword-inspire-theme",
            search_paths=[
                "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:descriptiveKeywords/gmd:MD_Keywords/gmd:keyword/gco:CharacterString/text()",
                "gmd:identificationInfo/srv:SV_ServiceIdentification/gmd:descriptiveKeywords/gmd:MD_Keywords/gmd:keyword/gco:CharacterString/text()",
            ],
            multiplicity="*",
        ),
        GeminiElement(
            name="keyword-controlled-other",
            search_paths=[
                "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:descriptiveKeywords/gmd:MD_Keywords/gmd:keyword/gco:CharacterString/text()",
                "gmd:identificationInfo/srv:SV_ServiceIdentification/gmd:descriptiveKeywords/gmd:MD_Keywords/gmd:keyword/gco:CharacterString/text()",
                "gmd:identificationInfo/srv:SV_ServiceIdentification/srv:keywords/gmd:MD_Keywords/gmd:keyword/gco:CharacterString/text()",
            ],
            multiplicity="*",
        ),
        GeminiElement(
            name="keyword-free-text",
            search_paths=[
            ],
            multiplicity="*",
        ),
        GeminiElement(
            name="limitations-on-public-access",
            search_paths=[
                "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:resourceConstraints/gmd:MD_LegalConstraints/gmd:otherConstraints/gco:CharacterString/text()",
                "gmd:identificationInfo/srv:SV_ServiceIdentification/gmd:resourceConstraints/gmd:MD_LegalConstraints/gmd:otherConstraints/gco:CharacterString/text()",
            ],
            multiplicity="1..*",
        ),
        GeminiElement(
            name="use-constraints",
            search_paths=[
                "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:resourceConstraints/gmd:MD_Constraints/gmd:useLimitation/gco:CharacterString/text()",
                "gmd:identificationInfo/srv:SV_ServiceIdentification/gmd:resourceConstraints/gmd:MD_Constraints/gmd:useLimitation/gco:CharacterString/text()",
            ],
            multiplicity="*",
        ),
        GeminiElement(
            name="spatial-data-service-type",
            search_paths=[
                "gmd:identificationInfo/srv:SV_ServiceIdentification/srv:serviceType/gco:LocalName/text()",
            ],
            multiplicity="0..1",
        ),
        GeminiElement(
            name="spatial-resolution",
            search_paths=[
                "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:spatialResolution/gmd:MD_Resolution/gmd:distance/gco:Distance",
                "gmd:identificationInfo/srv:SV_ServiceIdentification/gmd:spatialResolution/gmd:MD_Resolution/gmd:distance/gco:Distance",
            ],
            multiplicity="0..1",
        ),
        # GeminiElement(
        # name="spatial-resolution-units",
        # search_paths=[
        # "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:spatialResolution/gmd:MD_Resolution/gmd:distance/gco:Distance/@uom",
        # "gmd:identificationInfo/srv:SV_ServiceIdentification/gmd:spatialResolution/gmd:MD_Resolution/gmd:distance/gco:Distance/@uom",
        # ],
        # multiplicity="0..1",
        # ),
        GeminiElement(
            name="equivalent-scale",
            search_paths=[
                "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:spatialResolution/gmd:MD_Resolution/gmd:equivalentScale/gmd:MD_RepresentativeFraction/gmd:denominator/gco:Integer/text()",
                "gmd:identificationInfo/srv:SV_ServiceIdentification/gmd:spatialResolution/gmd:MD_Resolution/gmd:equivalentScale/gmd:MD_RepresentativeFraction/gmd:denominator/gco:Integer/text()",
            ],
            multiplicity="*",
        ),
        GeminiElement(
            name="dataset-language",
            search_paths=[
                "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:language/gmd:LanguageCode/@codeListValue",
                "gmd:identificationInfo/srv:SV_ServiceIdentification/gmd:language/gmd:LanguageCode/@codeListValue",
                "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:language/gmd:LanguageCode/text()",
                "gmd:identificationInfo/srv:SV_ServiceIdentification/gmd:language/gmd:LanguageCode/text()",
            ],
            multiplicity="*",
        ),
        GeminiElement(
            name="topic-category",
            search_paths=[
                "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:topicCategory/gmd:MD_TopicCategoryCode/text()",
                "gmd:identificationInfo/srv:SV_ServiceIdentification/gmd:topicCategory/gmd:MD_TopicCategoryCode/text()",
            ],
            multiplicity="*",
        ),
        GeminiElement(
            name="extent-controlled",
            search_paths=[
            ],
            multiplicity="*",
        ),
        GeminiElement(
            name="extent-free-text",
            search_paths=[
                "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:extent/gmd:EX_Extent/gmd:geographicElement/gmd:EX_GeographicDescription/gmd:geographicIdentifier/gmd:MD_Identifier/gmd:code/gco:CharacterString/text()",
                "gmd:identificationInfo/srv:SV_ServiceIdentification/srv:extent/gmd:EX_Extent/gmd:geographicElement/gmd:EX_GeographicDescription/gmd:geographicIdentifier/gmd:MD_Identifier/gmd:code/gco:CharacterString/text()",
            ],
            multiplicity="*",
        ),
        GeminiElement(
            name="bbox-west-long",
            search_paths=[
                "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:extent/gmd:EX_Extent/gmd:geographicElement/gmd:EX_GeographicBoundingBox/gmd:westBoundLongitude/gco:Decimal/text()",
                "gmd:identificationInfo/srv:SV_ServiceIdentification/srv:extent/gmd:EX_Extent/gmd:geographicElement/gmd:EX_GeographicBoundingBox/gmd:westBoundLongitude/gco:Decimal/text()",
            ],
            multiplicity="0..1",
        ),
        GeminiElement(
            name="bbox-east-long",
            search_paths=[
                "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:extent/gmd:EX_Extent/gmd:geographicElement/gmd:EX_GeographicBoundingBox/gmd:eastBoundLongitude/gco:Decimal/text()",
                "gmd:identificationInfo/srv:SV_ServiceIdentification/srv:extent/gmd:EX_Extent/gmd:geographicElement/gmd:EX_GeographicBoundingBox/gmd:eastBoundLongitude/gco:Decimal/text()",
            ],
            multiplicity="0..1",
        ),
        GeminiElement(
            name="bbox-north-lat",
            search_paths=[
                "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:extent/gmd:EX_Extent/gmd:geographicElement/gmd:EX_GeographicBoundingBox/gmd:northBoundLatitude/gco:Decimal/text()",
                "gmd:identificationInfo/srv:SV_ServiceIdentification/srv:extent/gmd:EX_Extent/gmd:geographicElement/gmd:EX_GeographicBoundingBox/gmd:northBoundLatitude/gco:Decimal/text()",
            ],
            multiplicity="0..1",
        ),
        GeminiElement(
            name="bbox-south-lat",
            search_paths=[
                "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:extent/gmd:EX_Extent/gmd:geographicElement/gmd:EX_GeographicBoundingBox/gmd:southBoundLatitude/gco:Decimal/text()",
                "gmd:identificationInfo/srv:SV_ServiceIdentification/srv:extent/gmd:EX_Extent/gmd:geographicElement/gmd:EX_GeographicBoundingBox/gmd:southBoundLatitude/gco:Decimal/text()",
            ],
            multiplicity="0..1",
        ),
        GeminiElement(
            name="temporal-extent-begin",
            search_paths=[
                "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:extent/gmd:EX_Extent/gmd:temporalElement/gmd:EX_TemporalExtent/gmd:extent/gml:TimePeriod/gml:beginPosition/text()",
                "gmd:identificationInfo/srv:SV_ServiceIdentification/srv:extent/gmd:EX_Extent/gmd:temporalElement/gmd:EX_TemporalExtent/gmd:extent/gml:TimePeriod/gml:beginPosition/text()",
            ],
            multiplicity="*",
        ),
        GeminiElement(
            name="temporal-extent-end",
            search_paths=[
                "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:extent/gmd:EX_Extent/gmd:temporalElement/gmd:EX_TemporalExtent/gmd:extent/gml:TimePeriod/gml:endPosition/text()",
                "gmd:identificationInfo/srv:SV_ServiceIdentification/srv:extent/gmd:EX_Extent/gmd:temporalElement/gmd:EX_TemporalExtent/gmd:extent/gml:TimePeriod/gml:endPosition/text()",
            ],
            multiplicity="*",
        ),
        GeminiElement(
            name="vertical-extent",
            search_paths=[
                "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:extent/gmd:EX_Extent/gmd:verticalElement/gmd:EX_VerticalExtent",
                "gmd:identificationInfo/srv:SV_ServiceIdentification/srv:extent/gmd:EX_Extent/gmd:verticalElement/gmd:EX_VerticalExtent",
            ],
            multiplicity="*",
        ),
        GeminiCoupledResources(
            name="coupled-resource",
            search_paths=[
                "gmd:identificationInfo/srv:SV_ServiceIdentification/srv:operatesOn",
            ],
            multiplicity="*",
        ),
# GeminiElement(
# name="coupled-resource",
# search_paths=[
# "gmd:identificationInfo/srv:SV_ServiceIdentification/srv:operatesOn/@xlink:href",
# ],
# multiplicity="*",
# ),
        GeminiElement(
            name="additional-information-source",
            search_paths=[
                "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:supplementalInformation/gco:CharacterString/text()",
            ],
            multiplicity="0..1",
        ),
        GeminiDataFormat(
            name="data-format",
            search_paths=[
                "gmd:distributionInfo/gmd:MD_Distribution/gmd:distributionFormat/gmd:MD_Format",
            ],
            multiplicity="*",
        ),
        GeminiResourceLocator(
            name="resource-locator",
            search_paths=[
                "gmd:distributionInfo/gmd:MD_Distribution/gmd:transferOptions/gmd:MD_DigitalTransferOptions/gmd:onLine/gmd:CI_OnlineResource",
            ],
            multiplicity="*",
        ),
        GeminiElement(
            name="conformity-specification",
            search_paths=[
                "gmd:dataQualityInfo/gmd:DQ_DataQuality/gmd:report/gmd:DQ_DomainConsistency/gmd:result/gmd:DQ_ConformanceResult/gmd:specification",
            ],
            multiplicity="0..1",
        ),
        GeminiElement(
            name="conformity-pass",
            search_paths=[
                "gmd:dataQualityInfo/gmd:DQ_DataQuality/gmd:report/gmd:DQ_DomainConsistency/gmd:result/gmd:DQ_ConformanceResult/gmd:pass/gco:Boolean/text()",
            ],
            multiplicity="0..1",
        ),
        GeminiElement(
            name="conformity-explanation",
            search_paths=[
                "gmd:dataQualityInfo/gmd:DQ_DataQuality/gmd:report/gmd:DQ_DomainConsistency/gmd:result/gmd:DQ_ConformanceResult/gmd:explanation/gco:CharacterString/text()",
            ],
            multiplicity="0..1",
        ),
        GeminiElement(
            name="lineage",
            search_paths=[
                "gmd:dataQualityInfo/gmd:DQ_DataQuality/gmd:lineage/gmd:LI_Lineage/gmd:statement/gco:CharacterString/text()",
            ],
            multiplicity="0..1",
        )
    ]

class InspireDocument(MappedXmlDocument):

    # Attribute specifications from "XPaths for GEMINI" by Peter Parslow.

    elements = [
        GeminiElement(
            name="guid",
            search_paths="gmd:fileIdentifier/gco:CharacterString/text()",
            multiplicity="0..1",
        ),
        GeminiElement(
            name="metadata-language",
            search_paths=[
                "gmd:language/gmd:LanguageCode/@codeListValue",
                "gmd:language/gmd:LanguageCode/text()",
            ],
            multiplicity="0..1",
        ),
        GeminiElement(
            name="resource-type",
            search_paths=[
                "gmd:hierarchyLevelName/gco:CharacterString/text()|gmd:hierarchyLevel/gmd:MD_ScopeCode/@codeListValue | gmd:hierarchyLevel/gmd:MD_ScopeCode/text()",
            ],
            multiplicity="1..*",
        ),
        GeminiResponsibleParty(
            name="metadata-point-of-contact",
            search_paths=[
                "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:pointOfContact/gmd:CI_ResponsibleParty",
                "gmd:identificationInfo/srv:SV_ServiceIdentification/gmd:pointOfContact/gmd:CI_ResponsibleParty",
            ],
            multiplicity="1..*",
        ),
        GeminiElement(
            name="metadata-date",
            search_paths=[
                "gmd:dateStamp/gco:DateTime/text()",
                "gmd:dateStamp/gco:Date/text()",
            ],
            multiplicity="1",
        ),
        GeminiElement(
            name="spatial-reference-system",
            search_paths=[
                "gmd:referenceSystemInfo/gmd:MD_ReferenceSystem/gmd:referenceSystemIdentifier/gmd:RS_Identifier/gmd:code/gco:CharacterString/text()",
            ],
            multiplicity="0..1",
        ),
        GeminiElement(
            name="title",
            search_paths=[
                "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:citation/gmd:CI_Citation/gmd:title/gco:CharacterString/text()",
                "gmd:identificationInfo/srv:SV_ServiceIdentification/gmd:citation/gmd:CI_Citation/gmd:title/gco:CharacterString/text()",
            ],
            multiplicity="1",
        ),
        GeminiElement(
            name="alternative-title",
            search_paths=[
                "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:citation/gmd:CI_Citation/gmd:alternativeTitle/gco:CharacterString/text()",
                "gmd:identificationInfo/srv:SV_ServiceIdentification/gmd:citation/gmd:CI_Citation/gmd:alternativeTitle/gco:CharacterString/text()",
            ],
            multiplicity="*",
        ),
        GeminiReferenceDate(
            name="dataset-date",
            search_paths=[
                "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:citation/gmd:CI_Citation/gmd:date/gmd:CI_Date",
                "gmd:identificationInfo/srv:SV_ServiceIdentification/gmd:citation/gmd:CI_Citation/gmd:date/gmd:CI_Date",
            ],
            multiplicity="*",
        ),
                
        GeminiReferenceDate(
            name="service-date",
            search_paths=[
                "gmd:identificationInfo/srv:SV_ServiceIdentification/gmd:citation/gmd:CI_Citation/gmd:date/gmd:CI_Date",
            ],
            multiplicity="*",
        ),
                
        # # Todo: Suggestion from PP not to bother pulling this into the package.
        # GeminiElement(
        # name="unique-resource-identifier",
        # search_paths=[
        # "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:citation/gmd:CI_Citation/gmd:identifier/gmd:RS_Identifier",
        # "gmd:identificationInfo/srv:SV_ServiceIdentification/gmd:citation/gmd:CI_Citation/gmd:identifier/gmd:RS_Identifier",
        # ],
        # multiplicity="1",
        # ),
        GeminiElement(
            name="abstract",
            search_paths=[
                "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:abstract/gco:CharacterString/text()",
                "gmd:identificationInfo/srv:SV_ServiceIdentification/gmd:abstract/gco:CharacterString/text()",
            ],
            multiplicity="1",
        ),
        GeminiResponsibleParty(
            name="responsible-organisation",
            search_paths=[
                "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:pointOfContact/gmd:CI_ResponsibleParty",
                "gmd:identificationInfo/srv:SV_ServiceIdentification/gmd:pointOfContact/gmd:CI_ResponsibleParty",
                "gmd:contact/gmd:CI_ResponsibleParty",
            ],
            multiplicity="1..*",
        ),
        GeminiElement(
            name="frequency-of-update",
            search_paths=[
                "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:resourceMaintenance/gmd:MD_MaintenanceInformation/gmd:maintenanceAndUpdateFrequency/gmd:MD_MaintenanceFrequencyCode/@codeListValue",
                "gmd:identificationInfo/srv:SV_ServiceIdentification/gmd:resourceMaintenance/gmd:MD_MaintenanceInformation/gmd:maintenanceAndUpdateFrequency/gmd:MD_MaintenanceFrequencyCode/@codeListValue",

                "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:resourceMaintenance/gmd:MD_MaintenanceInformation/gmd:maintenanceAndUpdateFrequency/gmd:MD_MaintenanceFrequencyCode/text()",
                "gmd:identificationInfo/srv:SV_ServiceIdentification/gmd:resourceMaintenance/gmd:MD_MaintenanceInformation/gmd:maintenanceAndUpdateFrequency/gmd:MD_MaintenanceFrequencyCode/text()",
            ],
            multiplicity="0..1",
        ),
        GeminiElement(
            name="frequency-of-update-factor",
            search_paths=[
                "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:resourceMaintenance/gmd:MD_MaintenanceInformation/gmd:userDefinedMaintenanceFrequency/gts:TM_PeriodDuration/text()",
                "gmd:identificationInfo/srv:SV_ServiceIdentification/gmd:resourceMaintenance/gmd:MD_MaintenanceInformation/gmd:userDefinedMaintenanceFrequency/gts:TM_PeriodDuration/text()",
            ],
            multiplicity="0..1",
        ),
                              
        GeminiElement(
            name="keyword-inspire-theme",
            search_paths=[
                "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:descriptiveKeywords/gmd:MD_Keywords/gmd:keyword/gco:CharacterString/text()",
                "gmd:identificationInfo/srv:SV_ServiceIdentification/gmd:descriptiveKeywords/gmd:MD_Keywords/gmd:keyword/gco:CharacterString/text()",
            ],
            multiplicity="*",
        ),
        GeminiElement(
            name="keyword-controlled-other",
            search_paths=[
                "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:descriptiveKeywords/gmd:MD_Keywords/gmd:keyword/gco:CharacterString/text()",
                "gmd:identificationInfo/srv:SV_ServiceIdentification/gmd:descriptiveKeywords/gmd:MD_Keywords/gmd:keyword/gco:CharacterString/text()",
                "gmd:identificationInfo/srv:SV_ServiceIdentification/srv:keywords/gmd:MD_Keywords/gmd:keyword/gco:CharacterString/text()",
            ],
            multiplicity="*",
        ),
        GeminiElement(
            name="keyword-free-text",
            search_paths=[
            ],
            multiplicity="*",
        ),
        GeminiElement(
            name="access-constraints",
            search_paths=[
                "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:resourceConstraints/gmd:MD_LegalConstraints/gmd:accessConstraints/gmd:MD_RestrictionCode/text()",
                "gmd:identificationInfo/srv:SV_ServiceIdentification/gmd:resourceConstraints/gmd:MD_LegalConstraints/gmd:accessConstraints/gmd:MD_RestrictionCode/text()",
            ],
            multiplicity="*",
        ),
        GeminiElement(
            name="use-constraints",
            search_paths=[
                "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:resourceConstraints/gmd:MD_LegalConstraints/gmd:useConstraints/gmd:MD_RestrictionCode/text()",
                "gmd:identificationInfo/srv:SV_ServiceIdentification/gmd:resourceConstraints/gmd:MD_LegalConstraints/gmd:useConstraints/gmd:MD_RestrictionCode/text()",
            ],
            multiplicity="*",
        ),
        GeminiElement(
            name="other-constraints",
            search_paths=[
                "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:resourceConstraints/gmd:MD_LegalConstraints/gmd:otherConstraints/gco:CharacterString/text()",
                "gmd:identificationInfo/srv:SV_ServiceIdentification/gmd:resourceConstraints/gmd:MD_LegalConstraints/gmd:otherConstraints/gco:CharacterString/text()",
            ],
            multiplicity="*",
        ),
        GeminiElement(
            name="use-limitations",
            search_paths=[
                "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:resourceConstraints/gmd:MD_Constraints/gmd:useLimitation/gco:CharacterString/text()",
                "gmd:identificationInfo/srv:SV_ServiceIdentification/gmd:resourceConstraints/gmd:MD_Constraints/gmd:useLimitation/gco:CharacterString/text()",
            ],
            multiplicity="*",
        ),
        GeminiElement(
            name="spatial-data-service-type",
            search_paths=[
                "gmd:identificationInfo/srv:SV_ServiceIdentification/srv:serviceType/gco:LocalName/text()",
            ],
            multiplicity="0..1",
        ),
        GeminiElement(
            name="spatial-resolution",
            search_paths=[
                "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:spatialResolution/gmd:MD_Resolution/gmd:distance/gco:Distance",
                "gmd:identificationInfo/srv:SV_ServiceIdentification/gmd:spatialResolution/gmd:MD_Resolution/gmd:distance/gco:Distance",
            ],
            multiplicity="0..1",
        ),
        # GeminiElement(
        # name="spatial-resolution-units",
        # search_paths=[
        # "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:spatialResolution/gmd:MD_Resolution/gmd:distance/gco:Distance/@uom",
        # "gmd:identificationInfo/srv:SV_ServiceIdentification/gmd:spatialResolution/gmd:MD_Resolution/gmd:distance/gco:Distance/@uom",
        # ],
        # multiplicity="0..1",
        # ),
        GeminiElement(
            name="equivalent-scale",
            search_paths=[
                "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:spatialResolution/gmd:MD_Resolution/gmd:equivalentScale/gmd:MD_RepresentativeFraction/gmd:denominator/gco:Integer/text()",
                "gmd:identificationInfo/srv:SV_ServiceIdentification/gmd:spatialResolution/gmd:MD_Resolution/gmd:equivalentScale/gmd:MD_RepresentativeFraction/gmd:denominator/gco:Integer/text()",
            ],
            multiplicity="*",
        ),
        GeminiElement(
            name="dataset-language",
            search_paths=[
                "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:language/gmd:LanguageCode/@codeListValue",
                "gmd:identificationInfo/srv:SV_ServiceIdentification/gmd:language/gmd:LanguageCode/@codeListValue",
                "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:language/gmd:LanguageCode/text()",
                "gmd:identificationInfo/srv:SV_ServiceIdentification/gmd:language/gmd:LanguageCode/text()",
            ],
            multiplicity="*",
        ),
        GeminiElement(
            name="topic-category",
            search_paths=[
                "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:topicCategory/gmd:MD_TopicCategoryCode/text()",
                "gmd:identificationInfo/srv:SV_ServiceIdentification/gmd:topicCategory/gmd:MD_TopicCategoryCode/text()",
            ],
            multiplicity="*",
        ),
        GeminiElement(
            name="extent-controlled",
            search_paths=[
            ],
            multiplicity="*",
        ),
        GeminiElement(
            name="extent-free-text",
            search_paths=[
                "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:extent/gmd:EX_Extent/gmd:geographicElement/gmd:EX_GeographicDescription/gmd:geographicIdentifier/gmd:MD_Identifier/gmd:code/gco:CharacterString/text()",
                "gmd:identificationInfo/srv:SV_ServiceIdentification/srv:extent/gmd:EX_Extent/gmd:geographicElement/gmd:EX_GeographicDescription/gmd:geographicIdentifier/gmd:MD_Identifier/gmd:code/gco:CharacterString/text()",
            ],
            multiplicity="*",
        ),
        GeminiElement(
            name="bbox-west-long",
            search_paths=[
                "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:extent/gmd:EX_Extent/gmd:geographicElement/gmd:EX_GeographicBoundingBox/gmd:westBoundLongitude/gco:Decimal/text()",
                "gmd:identificationInfo/srv:SV_ServiceIdentification/srv:extent/gmd:EX_Extent/gmd:geographicElement/gmd:EX_GeographicBoundingBox/gmd:westBoundLongitude/gco:Decimal/text()",
            ],
            multiplicity="0..1",
        ),
        GeminiElement(
            name="bbox-east-long",
            search_paths=[
                "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:extent/gmd:EX_Extent/gmd:geographicElement/gmd:EX_GeographicBoundingBox/gmd:eastBoundLongitude/gco:Decimal/text()",
                "gmd:identificationInfo/srv:SV_ServiceIdentification/srv:extent/gmd:EX_Extent/gmd:geographicElement/gmd:EX_GeographicBoundingBox/gmd:eastBoundLongitude/gco:Decimal/text()",
            ],
            multiplicity="0..1",
        ),
        GeminiElement(
            name="bbox-north-lat",
            search_paths=[
                "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:extent/gmd:EX_Extent/gmd:geographicElement/gmd:EX_GeographicBoundingBox/gmd:northBoundLatitude/gco:Decimal/text()",
                "gmd:identificationInfo/srv:SV_ServiceIdentification/srv:extent/gmd:EX_Extent/gmd:geographicElement/gmd:EX_GeographicBoundingBox/gmd:northBoundLatitude/gco:Decimal/text()",
            ],
            multiplicity="0..1",
        ),
        GeminiElement(
            name="bbox-south-lat",
            search_paths=[
                "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:extent/gmd:EX_Extent/gmd:geographicElement/gmd:EX_GeographicBoundingBox/gmd:southBoundLatitude/gco:Decimal/text()",
                "gmd:identificationInfo/srv:SV_ServiceIdentification/srv:extent/gmd:EX_Extent/gmd:geographicElement/gmd:EX_GeographicBoundingBox/gmd:southBoundLatitude/gco:Decimal/text()",
            ],
            multiplicity="0..1",
        ),
        GeminiElement(
            name="temporal-extent-begin",
            search_paths=[
                "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:extent/gmd:EX_Extent/gmd:temporalElement/gmd:EX_TemporalExtent/gmd:extent/gml:TimePeriod/gml:beginPosition/text()",
                "gmd:identificationInfo/srv:SV_ServiceIdentification/srv:extent/gmd:EX_Extent/gmd:temporalElement/gmd:EX_TemporalExtent/gmd:extent/gml:TimePeriod/gml:beginPosition/text()",
            ],
            multiplicity="*",
        ),
        GeminiElement(
            name="temporal-extent-end",
            search_paths=[
                "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:extent/gmd:EX_Extent/gmd:temporalElement/gmd:EX_TemporalExtent/gmd:extent/gml:TimePeriod/gml:endPosition/text()",
                "gmd:identificationInfo/srv:SV_ServiceIdentification/srv:extent/gmd:EX_Extent/gmd:temporalElement/gmd:EX_TemporalExtent/gmd:extent/gml:TimePeriod/gml:endPosition/text()",
            ],
            multiplicity="*",
        ),
                
        GeminiElement(
            name="spatial-text",
            search_paths=[
                "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:extent/gmd:EX_Extent/gmd:geographicElement/gmd:EX_GeographicDescription/gmd:geographicIdentifier/gmd:MD_Identifier/gmd:code/gco:CharacterString/text()",
                "gmd:identificationInfo/srv:SV_ServiceIdentification/srv:extent/gmd:EX_Extent/gmd:geographicElement/gmd:EX_GeographicDescription/gmd:geographicIdentifier/gmd:MD_Identifier/gmd:code/gco:CharacterString/text()",
            ],

            multiplicity="0..1",
        ),
        GeminiElement(
            name="vertical-extent",
            search_paths=[
                "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:extent/gmd:EX_Extent/gmd:verticalElement/gmd:EX_VerticalExtent",
                "gmd:identificationInfo/srv:SV_ServiceIdentification/srv:extent/gmd:EX_Extent/gmd:verticalElement/gmd:EX_VerticalExtent",
            ],
            multiplicity="*",
        ),
        GeminiCoupledResources(
            name="coupled-resource",
            search_paths=[
                "gmd:identificationInfo/srv:SV_ServiceIdentification/srv:operatesOn",
            ],
            multiplicity="*",
        ),
# GeminiElement(
# name="coupled-resource",
# search_paths=[
# "gmd:identificationInfo/srv:SV_ServiceIdentification/srv:operatesOn/@xlink:href",
# ],
# multiplicity="*",
# ),
        GeminiElement(
            name="additional-information-source",
            search_paths=[
                "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:supplementalInformation/gco:CharacterString/text()",
            ],
            multiplicity="0..1",
        ),
        GeminiDataFormat(
            name="data-format",
            search_paths=[
                "gmd:distributionInfo/gmd:MD_Distribution/gmd:distributionFormat/gmd:MD_Format",
            ],
            multiplicity="*",
        ),
        GeminiResourceLocator(
            name="resource-locator",
            search_paths=[
                "gmd:distributionInfo/gmd:MD_Distribution/gmd:transferOptions/gmd:MD_DigitalTransferOptions/gmd:onLine/gmd:CI_OnlineResource",
            ],
            multiplicity="*",
        ),
        GeminiResourceLocator(
            name="service-locator",
            search_paths=[
                "gmd:identificationInfo/srv:SV_ServiceIdentification/srv:containsOperations/srv:SV_OperationMetadata/srv:connectPoint/gmd:CI_OnlineResource",
            ],
            multiplicity="*",
        ),
        GeminiElement(
            name="conformity-specification",
            search_paths=[
                "gmd:dataQualityInfo/gmd:DQ_DataQuality/gmd:report/gmd:DQ_DomainConsistency/gmd:result/gmd:DQ_ConformanceResult/gmd:specification",
            ],
            multiplicity="0..1",
        ),
        GeminiElement(
            name="conformity-pass",
            search_paths=[
                "gmd:dataQualityInfo/gmd:DQ_DataQuality/gmd:report/gmd:DQ_DomainConsistency/gmd:result/gmd:DQ_ConformanceResult/gmd:pass/gco:Boolean/text()",
            ],
            multiplicity="0..1",
        ),
        GeminiElement(
            name="conformity-explanation",
            search_paths=[
                "gmd:dataQualityInfo/gmd:DQ_DataQuality/gmd:report/gmd:DQ_DomainConsistency/gmd:result/gmd:DQ_ConformanceResult/gmd:explanation/gco:CharacterString/text()",
            ],
            multiplicity="0..1",
        ),
        GeminiElement(
            name="lineage",
            search_paths=[
                "gmd:dataQualityInfo/gmd:DQ_DataQuality/gmd:lineage/gmd:LI_Lineage/gmd:statement/gco:CharacterString/text()",
            ],
            multiplicity="0..1",
        ),
        GeminiElement(
            name="keyword-list-with-thesaurus",
            search_paths=[
                "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:descriptiveKeywords/gmd:MD_Keywords[gmd:thesaurusName/gmd:CI_Citation/gmd:title/gco:CharacterString/text()]",
                "gmd:identificationInfo/srv:SV_ServiceIdentification/gmd:descriptiveKeywords/gmd:MD_Keywords[gmd:thesaurusName/gmd:CI_Citation/gmd:title/gco:CharacterString/text()]",
                "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:descriptiveKeywords/gmd:MD_Keywords[gmd:thesaurusName/gmd:CI_Citation/gmd:title/gco:CharacterString/text()]",
                "gmd:identificationInfo/srv:SV_ServiceIdentification/gmd:descriptiveKeywords/gmd:MD_Keywords[gmd:thesaurusName/gmd:CI_Citation/gmd:title/gco:CharacterString/text()]",
                "gmd:identificationInfo/srv:SV_ServiceIdentification/srv:keywords/gmd:MD_Keywords[gmd:thesaurusName/gmd:CI_Citation/gmd:title/gco:CharacterString/text()]",
            ],
            multiplicity="*",
        ),
                        
         GeminiElement(
            name="keyword-list-without-thesaurus",
            search_paths=[
                "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:descriptiveKeywords/gmd:MD_Keywords[not(gmd:thesaurusName)]",
                "gmd:identificationInfo/srv:SV_ServiceIdentification/gmd:descriptiveKeywords/gmd:MD_Keywords[not(gmd:thesaurusName)]",
                "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:descriptiveKeywords/gmd:MD_Keywords[not(gmd:thesaurusName)]]",
                "gmd:identificationInfo/srv:SV_ServiceIdentification/gmd:descriptiveKeywords/gmd:MD_Keywords[not(gmd:thesaurusName)]]",
                "gmd:identificationInfo/srv:SV_ServiceIdentification/srv:keywords/gmd:MD_Keywords[not(gmd:thesaurusName)]]",
                
            ],
            multiplicity="*",
        )
    ]

    def infer_values(self, values):
        # Todo: Infer name.
        self.infer_date_released(values)
        self.infer_date_updated(values)
        self.infer_date_created(values)
        self.infer_special_url(values)
        self.infer_url(values)
        # Todo: Infer resources.
        self.infer_tags(values)
        self.infer_publisher(values)
        self.infer_contact(values)
        # self.infer_contact_email(values)
        self.infer_groups(values)
        self.infer_special_tags(values)
        self.infer_pointOfContact(values)
        
        return values

    def infer_date_released(self, values):
        value = []
        for key in ['service-date', 'dataset-date']:
            for date in values[key]:
                if date['type'] == 'publication' and date['value'] not in value:
                    value.append(date['value'])                
        values['date-released'] = value

    def infer_date_updated(self, values):
        value = []
        # Use last of several multiple revision dates.
        for key in ['service-date', 'dataset-date']:
            for date in values[key]:
                if date['type'] == 'revision' and date['value'] not in value:
                    value.append(date['value'])
        values['date-updated'] = value

    def infer_date_created(self, values):
        value = []
        for key in ['service-date', 'dataset-date']:
            for date in values[key]:
                if date['type'] == 'creation' and date['value'] not in value:
                    value.append(date['value'])
        values['date-created'] = value


    def infer_special_url(self, values):
        value = ''
        used_datasets = []
        found_resources = []
        for locator in values['resource-locator']:
            if ('Weitere Informationen' in locator['name'] and 'den Datensatz' in locator['name']) or ('URL zu weiteren Informationen' in locator['name'] and 'den Datensatz' in locator['name']):
                values['further_info'] = locator['url']  
                found_resources.append(locator) 
            if 'Basisdaten' in locator['name']:
                used_datasets.append(locator['name']) 
                found_resources.append(locator)              
        values['used_datasets'] = used_datasets 
        
        for resource in found_resources:
            values['resource-locator'].remove(resource)  
        

    def infer_url(self, values):
        value = ''
        for locator in values['resource-locator']:
            if locator['function'] == 'information':
                value = locator['url']
                break
        values['url'] = value

    def infer_tags(self, values):
        tags = []
        for key in ['keyword-inspire-theme', 'keyword-controlled-other', 'keyword-free-text']:
            for item in values[key]:
                if item not in tags:
                    #item = item.encode('utf8')
                    tags.append(item)
        values['tags'] = tags


    def infer_publisher(self, values):
        value = ''
        for responsible_party in values['responsible-organisation']:
            if responsible_party['role'] == 'publisher':
                value = responsible_party['organisation-name']
            if value:
                break
        values['publisher'] = value

    def infer_contact(self, values):
        value = ''
        for responsible_party in values['responsible-organisation']:
            value = responsible_party['organisation-name']
            if value:
                break
        values['contact'] = value
        
        
        
    #######################################################################################################    
    '''
        New added: 
            functions:                infer_special_tags, infer_groups, 
            InspireDocument elements: keyword-list-with-type, keyword-list-without-types
    '''    
    def infer_special_tags(self, values):
        
        tags = values['tags']
        # print tags
        size = len(values['keyword-list-with-thesaurus'])    
        xpath_keyword = "gmd:keyword/gco:CharacterString/text()"

        if size > 0:
            for item in values['keyword-list-without-thesaurus']:
                tree = self.get_xml_part_tree_(item)
                tag = tree.xpath(xpath_keyword, namespaces=GeminiElement.namespaces)
                for i in tag:
                    tags.append(i)
        else:          
            for key in ['keyword-inspire-theme', 'keyword-controlled-other', 'keyword-free-text']:
                for item in values[key]:
                    if item not in tags:
                        tags.append(item)
        
        # print tags                
        values['special_tags'] = tags
  
                  
    def infer_groups(self, values):
        
        import re
        
        groups = []
        tags = []
        xpath_type = "gmd:thesaurusName/gmd:CI_Citation/gmd:title/gco:CharacterString/text()"
        xpath_keyword = "gmd:keyword/gco:CharacterString/text()"

        for item in values['keyword-list-with-thesaurus']:
            tree = self.get_xml_part_tree_(item)
            type = tree.xpath(xpath_type, namespaces=GeminiElement.namespaces)
            
            
            if "OGDD-Kategorien" in type:
                tag = tree.xpath(xpath_keyword, namespaces=GeminiElement.namespaces)
                for i in tag:
                    i = re.sub('\n', '', i)
                    i = re.sub('\t', '', i)
                    groups.append(i)

            else:
                tag = tree.xpath(xpath_keyword, namespaces=GeminiElement.namespaces)
                for i in tag:
                    tags.append(i)
                                    
        values['groups'] = groups 
        values['tags'] = tags 
                 
    #################################################################################################################
    
        
    def infer_pointOfContact(self, values):


        for responsible_party in values['responsible-organisation']:
            
            if isinstance(responsible_party, dict) and \
               isinstance(responsible_party.get('contact-info'), dict):
                                       
                if responsible_party.has_key('role'):    
                     
                    country = ''
                    city = ''
                    deliveryPoint = ''
                    adminitrativeArea = ''
                    postalCode = ''
                    address = ''
                    email = ''
                    individual_name = ''
                    organisation_name = ''
                    position_name = ''
                    url = ''
                    
                    if responsible_party['contact-info'].has_key('country'):
                        country = responsible_party['contact-info']['country']
                        
                    if responsible_party['contact-info'].has_key('city'):
                        city = responsible_party['contact-info']['city']
                        
                    if responsible_party['contact-info'].has_key('url'):
                        url = responsible_party['contact-info']['url']
                        
                    if responsible_party['contact-info'].has_key('adminitrativeArea'):
                        adminitrativeArea = responsible_party['contact-info']['adminitrativeArea']    
      
                    if responsible_party['contact-info'].has_key('deliveryPoint'):
                        deliveryPoint = responsible_party['contact-info']['deliveryPoint'] 
                        
                    if responsible_party['contact-info'].has_key('postalCode'):
                        postalCode = responsible_party['contact-info']['postalCode']
                         
                    if responsible_party['contact-info'].has_key('email'):
                        email = responsible_party['contact-info']['email']
                    
                    if responsible_party.has_key('individual-name'):
                        individual_name = responsible_party['individual-name']
                        
                    if responsible_party.has_key('organisation-name'):
                        organisation_name = responsible_party['organisation-name']
                                            
                    if responsible_party.has_key('position-name'):
                        position_name = responsible_party['position-name']
                    
                                       
                    if deliveryPoint and postalCode:                     
               
                        address = deliveryPoint
                        address = address + ', ' + postalCode
                            
                        if city:
                            address = address + ', ' + city
                            
                        if adminitrativeArea:
                            address = address + ', ' + adminitrativeArea
                            
                        if country:
                            address = address + ', ' + country
        
                    values[responsible_party['role'] + '-address'] = address 
                    values[responsible_party['role'] + '-email'] = email
                    values[responsible_party['role'] + '-individual-name'] = individual_name
                    values[responsible_party['role'] + '-organisation-name'] = organisation_name
                    values[responsible_party['role'] + '-position-name'] = position_name    
                    values[responsible_party['role'] + '-url'] = url    
                    # print responsible_party['role']     

    def infer_contact_email(self, values):
        value = ''
        for responsible_party in values['responsible-organisation']:
            if isinstance(responsible_party, dict) and \
               isinstance(responsible_party.get('contact-info'), dict) and \
               responsible_party['contact-info'].has_key('email'):
                value = responsible_party['contact-info']['email']
                if value:
                    break
        values['contact-email'] = value
        values['owner-email'] = ''
        values['publisher-email'] = ''

        for responsible_party in values['responsible-organisation']:
                if 'email' in responsible_party['contact-info']:
                        values['owner-email'] = responsible_party['contact-info']['email']
                        if responsible_party['role'] == 'publisher':
                                values['publisher-email'] = responsible_party['contact-info']['email']

#################################################################################################################
    
    def get_xml_part_tree_(self, xml_str):

        parser = etree.XMLParser(remove_blank_text=True)
        if type(xml_str) == unicode:
            return self.xml_str.encode('utf8')
        else:
            return etree.fromstring(xml_str, parser=parser)


  







