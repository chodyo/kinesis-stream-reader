module.exports = {
    filterRecords: filterRecords
};

function filterRecords(query, listOfObjects) {
    // filter the records only if the filter was asked for in the URL query params
    if (query.contactId) {
        listOfObjects = listOfObjects.filter(function (record) {
            const contactId = parseInt(query.contactId);
            try {
                // holy cow this is ugly. i wish people knew how to design json correctly.
                // since there can be two different contact IDs, check them both.
                // but look out! if there's no value it'll look like {key: null} while having a value looks like {key: {long: ###}}
                const contactObj = record.baseEventData["com.incontact.datainfra.events.ContactEvent"].mediaScopeIdentification.contactIdentification;
                return (contactObj.contactId && contactObj.contactId.long === contactId) ||
                    (contactObj.contactIdAlt && contactObj.contactIdAlt.long === contactId);
            } catch (err) {
                return false;
            }
        });
    }
    if (query.agentId) {
        listOfObjects = listOfObjects.filter(function (record) {
            const agentId = parseInt(query.agentId);
            try {
                const agentIdObj = record.baseEventData["com.incontact.datainfra.events.AgentEvent"].agentShiftIdentification.agentIdentification;
                return (agentIdObj.agentId && agentIdObj.agentId.long === agentId) ||
                    (agentIdObj.agentIdAlt && agentIdObj.agentIdAlt.long === agentId);
            } catch (err) {
                return false;
            }
        });
    }
    if (query.serverName) {
        listOfObjects = listOfObjects.filter(function (record) {
            try {
                return record.tenantId.serverName.string.toLowerCase() === query.serverName.toLowerCase();
            } catch (err) {
                return false;
            }
        });
    }
    if (query.tenantId) {
        listOfObjects = listOfObjects.filter(function (record) {
            const tenantId = parseInt(query.tenantId);
            try {
                const tenantIdObj = record.tenantId;
                return (tenantIdObj.tenantId && tenantIdObj.tenantId.long === tenantId) ||
                    (tenantIdObj.tenantIdAlt && tenantIdObj.tenantIdAlt.long === tenantId);
            } catch (err) {
                return false;
            }
        });
    }
    if (query.agentShiftId) {
        const agentShiftId = parseInt(query.agentShiftId);
        listOfObjects = listOfObjects.filter(function (record) {
            try {
                const agentShiftIdObj = record.baseEventData["com.incontact.datainfra.events.AgentEvent"].agentShiftIdentification;
                return (agentShiftIdObj.agentShiftId && agentShiftIdObj.agentShiftId.long === agentShiftId) ||
                    (agentShiftIdObj.agentShiftIdAlt && agentShiftIdObj.agentShiftIdAlt.long === agentShiftId);
            } catch (err) {
                return false;
            }
        });
    }
    return listOfObjects;
}