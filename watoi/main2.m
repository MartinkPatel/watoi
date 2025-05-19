// main.m and interface declarations would largely remain the same.
// WAMsgType enum might need updates based on new media_wa_type values.

@implementation Importer

// ... (initializeCoreDataWithMomd, initializeAndroidStoreFromPath, other helpers like convertAndroidTimestamp, addMissingMember, saveCoreData, loadChats, guessOurJID remain similar, but addMissingMember might need to handle JID lookups if only row_ids are passed)

// Helper method to get JID string from a row_id (if needed)
- (NSString *)getJIDStringFromRowID:(NSNumber *)rowID {
    if (!rowID || [rowID isEqual:[NSNull null]]) return nil;
    NSString *query = [NSString stringWithFormat:@"SELECT raw_string FROM jid WHERE _id = %@ LIMIT 1;", rowID];
    NSMutableArray *result = [self executeQuery:query]; // executeQuery needs to handle single value return
    if (result.count > 0 && [result.firstObject objectForKey:@"raw_string"]) {
        return [result.firstObject objectForKey:@"raw_string"];
    }
    NSLog(@"Warning: Could not find JID string for row_id: %@", rowID);
    NSLog(@"Warning getJIDStringFromRowID: Result: %@", result);
    return nil; // Or handle error appropriately
}


- (void) importChats {
    // Assuming 'chat' table replaces 'chat_view'
    // And 'creation' replaces 'created_timestamp'
    // And 'jid' replaces 'raw_string_jid'
    // Filter for 'hidden' might need to be re-thought or removed if 'chat' table only contains active chats
    NSArray *androidChats = [self executeQuery:@"SELECT _id, jid, archived, subject, creation FROM chat"]; // Added _id for potential FK use
    NSNull *null = [NSNull null];
    NSString *ourJID = nil;

    [self loadChats]; // Assumes iOS side still uses WAChatSession, WAGroupInfo etc.
    ourJID = [self guessOurJID];

    for (NSDictionary *achat in androidChats) {
        NSString *chatJID = [achat objectForKey:@"jid"]; // Modern: 'jid'
        if (!chatJID || [chatJID isEqual:null]) {
            NSLog(@"Skipping chat with NULL JID: %@", achat);
            continue;
        }
        //NSNumber *chatRowID = [achat objectForKey:@"_id"]; // Might be needed for message linking

        NSManagedObject *chat = [self.chats objectForKey:chatJID];
        NSMutableDictionary *members = nil;
        BOOL isGroup = NO; // Determine this based on chatJID format or presence of subject

        // Basic group detection (could be more robust, e.g., checking chatJID suffix like @g.us)
        isGroup = ([chatJID containsString:@"@g.us"] || [achat objectForKey:@"subject"] != null);


        if (chat == nil) {
            NSLog(@"%@: not found, creating new iOS chat session", chatJID);
            chat = [NSEntityDescription insertNewObjectForEntityForName:@"WAChatSession"
                                                 inManagedObjectContext:self.moc];
            [chat setValue:chatJID forKey:@"contactJID"];

            // Modern: 'archived' is likely an integer 0/1
            NSNumber *archivedNum = [achat objectForKey:@"archived"];
            BOOL isArchived = (archivedNum && ![archivedNum isEqual:null] && [archivedNum intValue] == 1);
            [chat setValue:[NSNumber numberWithBool:isArchived] forKey:@"archived"];

            [chat setValue:@0 forKey:@"messageCounter"]; // Will be updated later

            NSString *partnerName = [achat objectForKey:@"subject"];
            if (!isGroup) {
                // For non-groups, partnerName might be derived or fetched differently
                // The old code derived from JID if subject was null.
                if (partnerName == null || [partnerName isEqual:null]) {
                     partnerName = [chatJID componentsSeparatedByString:@"@"][0];
                }
            }
            [chat setValue:partnerName forKey:@"partnerName"];
            [self.chats setObject:chat forKey:chatJID];

            if (!isGroup) {
                continue;
            }

            NSManagedObject *group = [NSEntityDescription insertNewObjectForEntityForName:@"WAGroupInfo"
                                                                   inManagedObjectContext:self.moc];
            // Modern: 'creation' timestamp for group
            NSNumber *creationTimestamp = [achat objectForKey:@"creation"];
            if (creationTimestamp && ![creationTimestamp isEqual:null]) {
                NSDate *creationDate = [self convertAndroidTimestamp:creationTimestamp]; // convertAndroidTimestamp might need to handle seconds too
                [group setValue:creationDate forKey:@"creationDate"];
            }
            [group setValue:chat forKey:@"chatSession"];

            members = [NSMutableDictionary new];
            [self.chatMembers setObject:members forKey:chatJID];
        } else {
           
        }

        if (!isGroup) {
            continue;
        }
        NSLog(@"\t %@ is a group chat", chatJID);

        // Insert group members - Assuming 'group_participant_user' table
        // And 'group_jid' for group ID, 'user_jid' for member JID
        NSString *query = [NSString stringWithFormat:@"SELECT user_jid, admin FROM group_participant_user WHERE group_jid = '%@'", chatJID];
        NSMutableArray *amembers = [self executeQuery:query];

        for (NSDictionary *amember in amembers) {
            NSString *memberJID = [amember objectForKey:@"user_jid"]; // Modern: 'user_jid'
            if (!memberJID || [memberJID isEqual:null] || [memberJID isEqualToString:@""]) {
                // Handle case where our own JID might be represented differently or missing
                // The old code used "" to mean our JID. This might change.
                memberJID = ourJID;
                 if (!memberJID) {
                    NSLog(@"\tSkipping member with invalid JID for group %@: %@", chatJID, amember);
                    continue;
                }
            }

            NSManagedObject *member = [members objectForKey:memberJID];
            if (member == nil) {
                NSLog(@"\t not found member %@, adding.", memberJID);
                // 'admin' is likely an integer 0/1/2. The old code checked for NSNull.
                NSNumber *isAdminNum = [amember objectForKey:@"admin"];
                BOOL isAdmin = NO;
                if (isAdminNum && ![isAdminNum isEqual:null] && [isAdminNum intValue] > 0) { // 1 for admin, 2 for superadmin
                    isAdmin = YES;
                }
                [self addMissingMember:memberJID
                                toChat:chatJID
                               asAdmin:[NSNumber numberWithBool:isAdmin]]; // addMissingMember might need adjustment
            } else {
                 // Optionally update existing member's admin status
                NSNumber *isAdminNum = [amember objectForKey:@"admin"];
                if (isAdminNum && ![isAdminNum isEqual:null]) {
                    BOOL isAdmin = [isAdminNum intValue] > 0;
                    [member setValue:[NSNumber numberWithBool:isAdmin] forKey:@"isAdmin"];
                }
            }
        }
    }

    [self saveCoreData];
    NSLog(@"Loaded/Updated %lu chat(s)", (unsigned long)[self.chats count]); // Count from self.chats as it reflects processed chats
}


- (void) importMessages {
    // ASSUMPTIONS:
    // - Main table is 'message'.
    // - Chat link is 'chat_row_id' -> 'chat._id'. We need the chat JID.
    // - Sender in group is 'sender_jid_row_id' -> 'jid._id' -> 'jid.raw_string'.
    // - Text is in 'text_data'.
    // - 'message_type' might replace 'status' for filtering system messages.
    // - 'from_me' replaces 'key_from_me'.
    // - 'media_wa_type' still exists for media type.
    // - 'key_id' still exists for stanzaID.

    // This query becomes much more complex if we need to JOIN to get all necessary JIDs
    // For simplicity here, we'll assume we query per chat, and might do sub-queries or lookups.
    // A more efficient way would be a larger JOIN query.

    // Example: If we iterate chats and get chat._id (chatRowIDFromAndroid)
    // NSString *query = [NSString stringWithFormat:
    //    @"SELECT m.from_me, m.timestamp, m.key_id, m.message_type, m.media_wa_type, m.text_data, m.media_caption, m.sender_jid_row_id "
    //     "FROM message m "
    //     "WHERE m.chat_row_id = %@ AND m.message_type != %d " // %d = SOME_SYSTEM_MESSAGE_TYPE_CODE
    //     "ORDER BY m.timestamp", chatRowIDFromAndroid, SOME_SYSTEM_MESSAGE_TYPE_CODE];

    id null = [NSNull null];

    for (NSString *chatJID in self.chats) { @autoreleasepool {
        NSManagedObject *chat = [self.chats objectForKey:chatJID];
        NSDictionary *members = [self.chatMembers objectForKey:chatJID]; // From loadChats/importChats
        BOOL isGroup = ([chat valueForKey:@"groupInfo"] != nil);
        NSManagedObject *msg = nil;

        NSLog(@"Importing messages for chat: %@", [chat valueForKey:@"contactJID"]);

        // To get chat_row_id for the current chatJID to query messages:
        // This is inefficient if done per chat. Better to fetch all chat _ids initially.
        // Or pass chatRowID from importChats if available.
        NSNumber *androidChatRowID = nil;
        NSString *chatIdQuery = [NSString stringWithFormat:@"SELECT _id FROM chat WHERE jid = '%@' LIMIT 1;", chatJID];
        NSMutableArray *chatIdResult = [self executeQuery:chatIdQuery];
        if (chatIdResult.count > 0 && [chatIdResult.firstObject objectForKey:@"_id"]) {
            androidChatRowID = [chatIdResult.firstObject objectForKey:@"_id"];
        } else {
            NSLog(@"Could not find _id for chatJID: %@. Skipping messages.", chatJID);
            continue;
        }
        
        // Define a placeholder for system message type to ignore (this needs to be researched from the actual DB)
        // For example, if type 10 is system messages you want to ignore:
        int SYSTEM_MESSAGE_TYPE_TO_IGNORE = 6; // THIS IS A PLACEHOLDER

        NSString *messagesQuery = [NSString stringWithFormat:
            @"SELECT from_me, timestamp, key_id, message_type, text_data, sender_jid_row_id "
             "FROM message "
             "WHERE chat_row_id = %@ AND (message_type IS NULL OR message_type != %d) " // Filter out specific system messages
             "ORDER BY timestamp", androidChatRowID, SYSTEM_MESSAGE_TYPE_TO_IGNORE];

        NSMutableArray *results = [self executeQuery:messagesQuery];
        int sort = 0; // This will be reset by the later fetch and update loop

        for (NSDictionary *amsg in results) {
            msg = [NSEntityDescription insertNewObjectForEntityForName:@"WAMessage"
                                                inManagedObjectContext:self.moc];

            // Modern: 'from_me' is likely an integer 0/1
            BOOL fromMe = [[amsg objectForKey:@"from_me"] intValue] == 1;

            NSDate *timestamp = [self convertAndroidTimestamp:[amsg objectForKey:@"timestamp"]];
            [msg setValue:timestamp forKey:@"messageDate"];
            // TODO sentDate

            [msg setValue:[NSNumber numberWithBool:fromMe] forKey:@"isFromMe"];
            if (!fromMe) {
                [msg setValue:chatJID forKey:@"fromJID"]; // The JID of the chat partner or group
                if (isGroup) {
                    NSNumber *senderRowID = [amsg objectForKey:@"sender_jid_row_id"];
                    NSString *senderJID = nil;
                    if (senderRowID && ![senderRowID isEqual:null]) {
                        senderJID = [self getJIDStringFromRowID:senderRowID]; // Helper to lookup JID
                    } else {
                        // If sender_jid_row_id is NULL in a group message not from me,
                        // it's an anomaly or an old system message.
                        // The original code used chatJID for fromJID.
                        // For group messages, if sender is unknown, it's problematic.
                        NSLog(@"\tWarning: Group message in %@ not from me, but sender_jid_row_id is NULL. amsg: %@", chatJID, amsg);
                        // Fallback or skip? For now, let's try to add a placeholder member if JID is missing.
                        // senderJID might remain nil.
                    }

                    NSManagedObject *member = nil;
                    if (senderJID) {
                         member = [members objectForKey:senderJID];
                    }

                    if (member == nil && senderJID) { // Only add if we have a senderJID
                        NSLog(@"\tmissing sender %@ (from row_id %@), adding for chat %@", senderJID, senderRowID, chatJID);
                        member = [self addMissingMember:senderJID toChat:chatJID asAdmin:@NO];
                    } else if (member == nil && !senderJID && isGroup) {
                        NSLog(@"\tCannot determine sender for group message in %@. Message data: %@", chatJID, amsg);
                        // Decide how to handle: skip message, assign to a generic "unknown sender", etc.
                        // For now, it won't have a groupMember set.
                    }
                    if (member) { // Only set if member is found/created
                        [msg setValue:member forKey:@"groupMember"];
                    }
                }
            } else { // Message is from me
                [msg setValue:chatJID forKey:@"toJID"]; // The JID of the chat partner or group
                // Delivered? Status 5 was used. This needs to be mapped from new 'status' or 'message_type' columns.
                // For now, let's keep it as 5, but this is a guess.
                [msg setValue:@5 forKey:@"messageStatus"];
            }

            // Sort will be fixed later, but initialize for now
            [msg setValue:[NSNumber numberWithInt:0] forKey:@"sort"]; // Placeholder, will be updated

            // Modern: 'key_id' for stanzaID
            id stanzaID = [amsg objectForKey:@"key_id"];
            if (stanzaID && ![stanzaID isEqual:null]) {
                [msg setValue:stanzaID forKey:@"stanzaID"];
            }

            [msg setValue:@2 forKey:@"dataItemVersion"]; // Assuming this iOS-specific field remains

            // Message content
            // Modern: 'media_wa_type' for type, 'text_data' for text, 'media_caption'
            NSNumber *mediaTypeNum =  nil;
            WAMsgType type = MSG_TEXT; // Default
            if (mediaTypeNum && ![mediaTypeNum isEqual:null]) {
                type = [mediaTypeNum intValue];
                // IMPORTANT: The WAMsgType enum values (0 for text, 1 for image etc.)
                // might be different in newer Android DBs. This mapping needs verification.
            } else {
                // If media_wa_type is null, it's likely a text message or a system message
                // Check message_type if needed to further classify
                // NSNumber *generalMessageType = [amsg objectForKey:@"message_type"];
            }


            NSString *text = [amsg objectForKey:@"text_data"]; // Modern: 'text_data'

            if (type != MSG_TEXT) { // Assuming WAMsgType enum is still somewhat valid
                NSString *prefix = nil;
                switch (type) { // This switch relies on WAMsgType enum values matching new DB
                    case MSG_IMAGE: prefix = @"<image>"; break;
                    case MSG_AUDIO: prefix = @"<audio>"; break;
                    case MSG_VIDEO: prefix = @"<video>"; break;
                    case MSG_CONTACT: prefix = @"<contact>"; break;
                    case MSG_LOCATION: prefix = @"<location>"; break;
                    case MSG_CALL: prefix = @"<call>"; break; // Call log messages might be separate now
                    // MSG_WTF, MSG_WTF2 might map to new system message types
                    default: prefix = [NSString stringWithFormat:@"<unknown media_wa_type: %d>", type]; break;
                }

                NSString *caption = [amsg objectForKey:@"media_caption"];
                if (caption && ![caption isEqual:null] && [caption length] > 0) {
                    text = [NSString stringWithFormat:@"%@: %@", prefix, caption];
                } else {
                    text = prefix;
                }
            }

            [msg setValue:@(MSG_TEXT) forKey:@"messageType"]; // iOS side always gets MSG_TEXT for these placeholders

            if (text && ![text isEqual:null]) {
                [msg setValue:text forKey:@"text"];
            } else if (type != MSG_TEXT) { // If it was supposed to be media but text is still null
                 [msg setValue:[NSString stringWithFormat:@"<media type %d with no caption/text>", type] forKey:@"text"];
            }
            else {
                 // Truly null text for a text message (or unhandled system message)
                NSLog(@"Null text detected for message (not classified as media): %@", amsg);
                 [msg setValue:@"<message with null text>" forKey:@"text"]; // Placeholder for safety
            }

            [msg setValue:chat forKey:@"chatSession"];
        }

        // Fix sort fields (this logic can remain largely the same)
        // Fetch existing messages already in CoreData for this chat session
        NSFetchRequest *fetchRequest = [NSFetchRequest fetchRequestWithEntityName:@"WAMessage"];
        NSPredicate *predicate = [NSPredicate predicateWithFormat:@"chatSession = %@", [chat objectID]];
        NSSortDescriptor *sortDescriptor = [NSSortDescriptor sortDescriptorWithKey:@"messageDate" ascending:YES]; // Sort by date first
        // Then by original stanzaID or an imported order if available
        [fetchRequest setSortDescriptors:[NSArray arrayWithObject:sortDescriptor]];
        [fetchRequest setPredicate:predicate];
        // fetchRequest.includesPropertyValues = NO; // We need values to set sort
        fetchRequest.includesPendingChanges = YES; // Include newly added messages

        NSError *error = nil;
        NSArray *allMessagesForChat = [self.moc executeFetchRequest:fetchRequest error:&error];
        if (!allMessagesForChat) {
            NSLog(@"Error fetching messages for sort update: %@\n%@", [error localizedDescription], [error userInfo]);
            // Continue without sorting if fetch fails, or abort
        } else {
            int currentSortIndex = 0;
            for (NSManagedObject *messageToResort in allMessagesForChat) {
                [messageToResort setValue:[NSNumber numberWithInt:(currentSortIndex++)] forKey:@"sort"];
            }
            sort = currentSortIndex; // Update sort for chat's messageCounter
        }


        // When new message arrive, its sort field is taken from chat's counter
        [chat setValue:[NSNumber numberWithInt:sort] forKey:@"messageCounter"];

        // Link last message (fetch the actual last message by date from allMessagesForChat)
        if ([allMessagesForChat count] > 0) {
            NSManagedObject *lastMessageInChat = [allMessagesForChat lastObject]; // Since it's sorted by date
            [chat setValue:lastMessageInChat forKey:@"lastMessage"];
            [chat setValue:[lastMessageInChat valueForKey:@"text"] forKey:@"lastMessageText"];
            [chat setValue:[lastMessageInChat valueForKey:@"messageDate"] forKey:@"lastMessageDate"];
        } else if (msg != nil) { // Fallback if allMessagesForChat was empty but we processed one msg
             [chat setValue:msg forKey:@"lastMessage"];
             [chat setValue:[msg valueForKey:@"text"] forKey:@"lastMessageText"];
             [chat setValue:[msg valueForKey:@"messageDate"] forKey:@"lastMessageDate"];
        }


        [self saveCoreData];
    }}
}

// ... (peek* methods, dumpEntityDescriptions, executeQuery)
// executeQuery will be CRITICAL. It must correctly handle various data types from the new schema.
// It might need to be more robust in type conversion if a column can hold different types
// or if date/time formats change (e.g. seconds vs milliseconds for timestamps).

@end