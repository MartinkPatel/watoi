//
//  main.m
//  WhatsApp Android to iOS
//
//  Created by Anton S on 17/01/17.
//  Copyright © 2017 Anton S. All rights reserved.
//

#import <Foundation/Foundation.h>
#import <CoreData/CoreData.h>
#import <sqlite3.h>

typedef enum {
    // I saw a msg with type == -1
    MSG_TEXT = 0,
    MSG_IMAGE = 1,
    MSG_AUDIO = 2,
    MSG_VIDEO = 3,
    MSG_CONTACT = 4,
    MSG_LOCATION = 5,
    MSG_CALL = 8,
    MSG_WTF = 10,
    MSG_WTF2 = 13,
} WAMsgType;

@interface Importer : NSObject
- (void) initializeCoreDataWithMomd:(NSString *)momdPath andDatabase:(NSString *)dbPath;
- (void) initializeAndroidStoreFromPath:(NSString *)storePath;
- (void) import;
@end





int main(int argc, const char * argv[]) {
    if (argc != 4) {
        NSLog(@"usage: %s <android sqlite> <iphone sqlite> <momd>", argv[0]);
        return 1;
    }

    @autoreleasepool {
        NSMutableArray *args = [NSMutableArray new];
        for (unsigned int i = 1; i < argc; ++i) {
            [args addObject:[NSString stringWithUTF8String:argv[i]]];
        }

        Importer *imp = [Importer new];
        [imp initializeAndroidStoreFromPath:[args objectAtIndex:0]];
        [imp initializeCoreDataWithMomd:[args objectAtIndex:2]
                            andDatabase:[args objectAtIndex:1]];
        [imp import];
    }
    return 0;
}

// The meat

@interface Importer ()
@property (nonatomic, strong) NSManagedObjectContext *moc;
@property (nonatomic, strong) NSManagedObjectModel *mom;
@property (nonatomic, strong) NSPersistentStore *store;
@property (nonatomic) sqlite3 *androidStore;

@property (nonatomic, strong) NSMutableDictionary *chats;
@property (nonatomic, strong) NSMutableDictionary *chatMembers;

- (void) importChats;
- (void) importMessages;
- (void) saveCoreData;

- (void) loadChats;
- (NSString *) guessOurJID;
- (NSDate *) convertAndroidTimestamp:(NSNumber *)timestamp;
- (NSManagedObject *) addMissingMember:(NSString *)memberJID toChat:(NSString *)chatJID asAdmin:(NSNumber *)isAdmin;
- (NSString *) getJIDStringFromRowID:(NSNumber *)rowID;
- (NSNumber *)getJIDRowIDFromString:(NSString *)jidString;
// Debug stuff
- (void) dumpEntityDescriptions;
- (void) peekAndroidMessages;
- (void) peekiOSMessages;
@end

@implementation Importer

- (void) initializeCoreDataWithMomd:(NSString *)momdPath andDatabase:(NSString *)dbPath {
    NSFileManager *mgr = [NSFileManager defaultManager];
    BOOL isMomdADir = NO;

    NSLog(@"Probing WhatsAppChat.momd at path: %@", momdPath);
    if ([mgr fileExistsAtPath:momdPath isDirectory:&isMomdADir] && isMomdADir) {
        NSLog(@"    ok.");
    } else {
        NSLog(@"    missing/not a dir!");
        abort();
    }

    NSLog(@"Probing ChatStorage.sqlite at path: %@", dbPath);
    if ([mgr isReadableFileAtPath:dbPath]) {
        NSLog(@"    ok.");
    } else {
        NSLog(@"    missing/not writable!");
        abort();
    }

    NSURL *modelURL = [NSURL fileURLWithPath:momdPath];
    NSManagedObjectModel *mom = [[NSManagedObjectModel alloc] initWithContentsOfURL:modelURL];
    NSAssert(mom != nil, @"Error initializing Managed Object Model");
    self.mom = mom;

    NSPersistentStoreCoordinator *psc = [[NSPersistentStoreCoordinator alloc] initWithManagedObjectModel:mom];
    NSManagedObjectContext *moc = [[NSManagedObjectContext alloc] initWithConcurrencyType:NSMainQueueConcurrencyType];
    [moc setPersistentStoreCoordinator:psc];
    [moc setUndoManager:nil];
    self.moc = moc;

    NSError *error = nil;
    NSURL *storeURL = [NSURL fileURLWithPath:dbPath];
    NSDictionary *options = @{NSSQLitePragmasOption:@{@"journal_mode": @"DELETE"}};
    NSPersistentStore *store = [psc addPersistentStoreWithType:NSSQLiteStoreType configuration:nil
                                                           URL:storeURL options:options error:&error];
    NSAssert(store != nil, @"Error initializing PSC: %@\n%@", [error localizedDescription], [error userInfo]);

    self.store = store;
    NSLog(@"CoreData loaded");
}

- (void) initializeAndroidStoreFromPath:(NSString *)storePath {
    NSFileManager *mgr = [NSFileManager defaultManager];
    sqlite3 *store = nil;

    NSLog(@"Probing msgstore.db at path: %@", storePath);
    if ([mgr isReadableFileAtPath:storePath]) {
        NSLog(@"    ok.");
    } else {
        NSLog(@"    missing/not readable!");
        abort();
    }

    if (sqlite3_open([storePath UTF8String], &store) != SQLITE_OK) {
        NSLog(@"%s", sqlite3_errmsg(store));
    }

    NSLog(@"Android store loaded");
    self.androidStore = store;
}

- (void) import {
    [self importChats];
    [self importMessages];
}

- (void) dumpEntityDescriptions {
    for (NSEntityDescription* desc in self.mom.entities) {
        NSLog(@"%@\n\n", desc.name);
    }
}

- (void) peekiOSMessages {
    NSFetchRequest *fetchRequest = [NSFetchRequest fetchRequestWithEntityName:@"WAMessage"];
    fetchRequest.returnsObjectsAsFaults = NO;
    [fetchRequest setFetchLimit:100];

    NSError *error = nil;
    NSArray *results = [self.moc executeFetchRequest:fetchRequest error:&error];
    if (!results) {
        NSLog(@"Error fetching objects: %@\n%@", [error localizedDescription], [error userInfo]);
        abort();
    }

    for (NSManagedObject *msg in results) {
        NSString *sender = [[msg valueForKey:@"isFromMe"] intValue] ?
                            @"me" : [msg valueForKey:@"fromJID"];
        NSLog(@"%@: %@", sender, [msg valueForKey:@"text"]);
    }
}

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

- (NSMutableArray *) executeQuery:(NSString *)query {
    NSNull *null = [NSNull null];  // Stupid singleton
    NSMutableArray *results = [NSMutableArray new];
    NSMutableArray *columnNames = nil;
    sqlite3_stmt *prepared;
    int totalColumns = 0;
    int result = FALSE;

    result = sqlite3_prepare_v2(self.androidStore, [query UTF8String], -1, &prepared, NULL);
    totalColumns = sqlite3_column_count(prepared);
    if (result != SQLITE_OK) {
        NSLog(@"%s", sqlite3_errmsg(self.androidStore));
        abort();
    }

    // It's a SELECT
    if (totalColumns > 0) {
        columnNames = [NSMutableArray arrayWithCapacity:totalColumns];
        for (int i = 0; i < totalColumns; ++i) {
            [columnNames addObject:[NSString stringWithUTF8String:sqlite3_column_name(prepared, i)]];
        }
    }

    // Fetching rows one by one
    while ((result = sqlite3_step(prepared)) == SQLITE_ROW) {
        NSMutableArray *row = [NSMutableArray arrayWithCapacity:totalColumns];
        int columnsCount = sqlite3_data_count(prepared);

        for (int i = 0; i < columnsCount; ++i) {
            int columnType = sqlite3_column_type(prepared, i);
            NSObject *value = nil;

            switch (columnType) {
                case SQLITE_INTEGER:
                    value = [NSNumber numberWithLongLong:sqlite3_column_int64(prepared, i)];
                    break;
                case SQLITE_FLOAT:
                    value = [NSNumber numberWithDouble:sqlite3_column_double(prepared, i)];
                    break;
                case SQLITE_TEXT:
                    value = [NSString stringWithUTF8String:(const char *) sqlite3_column_text(prepared, i)];
                    break;
                case SQLITE_BLOB: // Ignore blobs for now
                case SQLITE_NULL:
                    break;
            }

            if (!value) {
                value = null;
            }

            [row addObject:value];
        }

        [results addObject:[NSDictionary dictionaryWithObjects:row
                                                       forKeys:columnNames]];
    }

    sqlite3_finalize(prepared);
    return results;
}

- (void) peekAndroidMessages {
    NSMutableArray *results = nil;
    results = [self executeQuery:@"SELECT * FROM messages LIMIT 100;"];

    for (NSDictionary *row in results) {
        NSString *sender = [[row objectForKey:@"key_from_me"] intValue] ?
                            @"me" : [row objectForKey:@"key_remote_jid"];
        NSLog(@"%@: %@", sender, [row objectForKey:@"data"]);
    }
}

- (NSDate *) convertAndroidTimestamp:(NSNumber *)timestamp {
    // It's stored in millis in android db
    return [NSDate dateWithTimeIntervalSince1970:([timestamp doubleValue] / 1000.0)];
}

- (NSManagedObject *) addMissingMember:(NSString *)memberJID toChat:(NSString *)chatJID asAdmin:(NSNumber *)isAdmin {
    NSManagedObject *member = [NSEntityDescription insertNewObjectForEntityForName:@"WAGroupMember"
                                                            inManagedObjectContext:self.moc];
    NSMutableDictionary * members = [self.chatMembers objectForKey:chatJID];
    NSManagedObject *chat = [self.chats objectForKey:chatJID];

    [member setValue:memberJID forKey:@"memberJID"];
    if (![isAdmin isKindOfClass:[NSNull class]]) {
        [member setValue:isAdmin forKey:@"isAdmin"];
    }
    // Active members were loaded from backup
    [member setValue:@NO forKey:@"isActive"];

    // FIXME Take it from wa.db of from other chats
    NSString *fakeContactName = [memberJID componentsSeparatedByString:@"@"][0];
    [member setValue:fakeContactName forKey:@"contactName"];

    // Associate with current chat
    [member setValue:chat forKey:@"chatSession"];
    [members setObject:member forKey:memberJID];

    return member;
}

- (void) importChats {
    NSArray *androidChats = [self executeQuery:@"SELECT _id, jid_row_id, archived, subject, created_timestamp FROM chat"]; // Added _id for potential FK use
    NSNull *null = [NSNull null];
    NSString *ourJID = nil;

    // Load chats from iOS backup - they contain some data,
    // that is hard/impossible to recover from Android backup.
    [self loadChats];
    // We'll need it on chat members import
    ourJID = [self guessOurJID];

    for (NSDictionary *achat in androidChats) {
        NSNumber *chatJID_1 = [achat objectForKey:@"jid_row_id"]; // Modern: 'jid'
        if (!chatJID_1 || [chatJID_1 isEqual:null]) {
            NSLog(@"Skipping chat with NULL JID: %@", achat);
            continue;
        }
        NSString * chatJID = [self getJIDStringFromRowID:chatJID_1];
        if (!chatJID) {
            NSLog(@"Skipping chat with NULL JID string: %@", achat);
            continue;
        }
        NSManagedObject *chat = [self.chats objectForKey:chatJID];
        NSMutableDictionary *members = nil;
        BOOL isGroup = FALSE;
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

            // Group chats should have associated GroupInfo objects
            NSManagedObject *group = [NSEntityDescription insertNewObjectForEntityForName:@"WAGroupInfo"
                                                                   inManagedObjectContext:self.moc];

            NSNumber *creationTimestamp = [achat objectForKey:@"created_timestamp"];
            if (creationTimestamp && ![creationTimestamp isEqual:null]) {
                NSDate *creationDate = [self convertAndroidTimestamp:creationTimestamp]; // convertAndroidTimestamp might need to handle seconds too
                [group setValue:creationDate forKey:@"creationDate"];
            }
            [group setValue:chat forKey:@"chatSession"];

            // Messages in groups are linked to members
            members = [NSMutableDictionary new];
            [self.chatMembers setObject:members forKey:chatJID];
        } else {
            NSLog(@"%@: found existing iOS chat session", chatJID);
            // Update existing chat if necessary (e.g., archived status, subject)
            NSNumber *archivedNum = [achat objectForKey:@"archived"];
            BOOL isArchived = (archivedNum && ![archivedNum isEqual:null] && [archivedNum intValue] == 1);
            [chat setValue:[NSNumber numberWithBool:isArchived] forKey:@"archived"];
            
            NSString *partnerName = [achat objectForKey:@"subject"];
            if (partnerName && ![partnerName isEqual:null]) {
                 [chat setValue:partnerName forKey:@"partnerName"];
            }


            isGroup = ([chat valueForKey:@"groupInfo"] != nil); // Check existing iOS groupInfo
            members = [self.chatMembers objectForKey:chatJID];
        }

        if (!isGroup) {
            continue;
        }
        NSLog(@"\t %@ is a group chat", chatJID);

        // Insert group members
        NSString *query = @"SELECT * from group_participants WHERE gjid == '%@'";
        NSMutableArray *amembers = [self executeQuery:[NSString stringWithFormat:query, chatJID]];
        for (NSDictionary *amember in amembers) {
            NSString *memberJID = [amember objectForKey:@"jid"];
            NSManagedObject *member = nil;

            if ([memberJID isEqualToString:@""]|| !memberJID || [memberJID isEqual:null]) {
                // This entry corresponds to our account, should add it as well.
                memberJID = ourJID;
            }

            // Check if this member was loaded from iOS backup
            member = [members objectForKey:memberJID];
            if (member == nil) {
                NSLog(@"\t not found member %@", memberJID);
                [self addMissingMember:memberJID
                                toChat:chatJID
                               asAdmin:[amember objectForKey:@"admin"]];
            }
        }
    }

    [self saveCoreData];
    NSLog(@"Loaded %lu chat(s)", (unsigned long)[androidChats count]);
}

- (void) loadChats {
    NSFetchRequest *fetchRequest = [NSFetchRequest fetchRequestWithEntityName:@"WAChatSession"];
    fetchRequest.returnsObjectsAsFaults = NO;

    NSError *error = nil;
    NSArray *results = [self.moc executeFetchRequest:fetchRequest error:&error];
    if (!results) {
        NSLog(@"Error fetching objects: %@\n%@", [error localizedDescription], [error userInfo]);
        abort();
    }

    NSMutableDictionary *chats = [NSMutableDictionary new];
    NSMutableDictionary *chatMembers = [NSMutableDictionary new];

    for (NSManagedObject *session in results) {
        NSString *chatJID = [session valueForKey:@"contactJID"];
        BOOL isGroup = ([session valueForKey:@"groupInfo"] != nil);

        [chats setObject:session forKey:chatJID];
        if (!isGroup) {
            continue;
        }

        // Messages in groups are linked to members
        NSMutableDictionary *membersDict = [NSMutableDictionary new];
        [chatMembers setObject:membersDict forKey:chatJID];

        NSSet *members = [session valueForKey:@"groupMembers"];
        for (NSManagedObject *member in members) {
            [membersDict setObject:member forKey:[member valueForKey:@"memberJID"]];
        }
    }

    self.chats = chats;
    self.chatMembers = chatMembers;
}

- (NSString *) guessOurJID {
    NSMutableDictionary *counts = [NSMutableDictionary new];

    for (NSDictionary *members in [self.chatMembers allValues]) {
        for (NSString *jid in members) {
            NSNumber *cnt = [counts objectForKey:jid];
            if (cnt == nil) {
                cnt = @0;
            }

            [counts setObject:[NSNumber numberWithInteger:([cnt integerValue] + 1)]
                       forKey:jid];
        }
    }

    // Our jid is present in every chat - should be most frequent one
    return [[counts keysSortedByValueUsingSelector:@selector(compare:)] lastObject];
}
- (NSNumber *)getJIDRowIDFromString:(NSString *)jidString {
    if (!jidString || [jidString isKindOfClass:[NSNull class]]) return nil;

    NSString *query = [NSString stringWithFormat:@"SELECT _id FROM jid WHERE raw_string = '%@' LIMIT 1;", jidString];
    NSMutableArray *result = [self executeQuery:query];

    if (result.count > 0 && [result.firstObject objectForKey:@"_id"]) {
        return [result.firstObject objectForKey:@"_id"];
    }

    NSLog(@"Warning: Could not find row_id for JID string: %@", jidString);
    NSLog(@"Warning getJIDRowIDFromString: Result: %@", result);
    return nil;
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
    NSUInteger totalChats = self.chats.count;
    NSUInteger index = 0;
    NSUInteger msgCount = 0;
    for (NSString *chatJID in self.chats) { @autoreleasepool {
        NSManagedObject *chat = [self.chats objectForKey:chatJID];
        NSDictionary *members = [self.chatMembers objectForKey:chatJID]; // From loadChats/importChats
        BOOL isGroup = ([chat valueForKey:@"groupInfo"] != nil);
        NSManagedObject *msg = nil;
        index++;
        if (index > 300) {
            NSLog(@"Breaking after 1000 messages");
            continue;
        }
        NSLog(@"[%lu/%lu] Importing messages for chat: %@", (unsigned long)index, (unsigned long)totalChats, chatJID);
        NSLog(@"Importing messages for chat: %@", [chat valueForKey:@"contactJID"]);

        // To get chat_row_id for the current chatJID to query messages:
        // This is inefficient if done per chat. Better to fetch all chat _ids initially.
        // Or pass chatRowID from importChats if available.
        NSNumber *androidChatRowID = nil;
        NSNumber *jidRowID = [self getJIDRowIDFromString:chatJID];
        if (!jidRowID || [jidRowID isEqual:null]) {
            NSLog(@"Could not find row_id for chatJID: %@. Skipping messages.", chatJID);
            continue;
        }
        NSString *chatIdQuery = [NSString stringWithFormat:@"SELECT _id FROM chat WHERE jid_row_id = '%@' LIMIT 1;", jidRowID];
        NSMutableArray *chatIdResult = [self executeQuery:chatIdQuery];
        if (chatIdResult.count > 0 && [chatIdResult.firstObject objectForKey:@"_id"]) {
            androidChatRowID = [chatIdResult.firstObject objectForKey:@"_id"];
        } else {
            NSLog(@"Could not find _id for chatJID: %@. Skipping messages.", chatJID);
            continue;
        }
        
        // Define a placeholder for system message type to ignore (this needs to be researched from the actual DB)
        // For example, if type 10 is system messages you want to ignore:
        int SYSTEM_MESSAGE_TYPE_TO_IGNORE = 7; // THIS IS A PLACEHOLDER
        int MESSAGE_TYPE_TEXT = 0; // Assuming 0 is the type for text messages
        NSString *messagesQuery = [NSString stringWithFormat:
            @"SELECT from_me, timestamp, key_id, message_type, text_data, sender_jid_row_id "
             "FROM message "
             "WHERE chat_row_id = %@ AND (message_type IS NULL OR message_type = %d) " // Filter out specific system messages
             "ORDER BY timestamp", androidChatRowID, MESSAGE_TYPE_TEXT];

        

        NSMutableArray *results = [self executeQuery:messagesQuery];
        int sort = 0; // This will be reset by the later fetch and update loop
        NSUInteger totalMessages = results.count;
        NSUInteger msgIndex = 0;
        for (NSDictionary *amsg in results) {
            msg = [NSEntityDescription insertNewObjectForEntityForName:@"WAMessage"
                                                inManagedObjectContext:self.moc];

            NSDate *timestamp = [self convertAndroidTimestamp:[amsg objectForKey:@"timestamp"]];
            msgIndex++;
            msgCount++;
            NSLog(@"[%lu/%lu] Importing message with Date: %@", (unsigned long)msgIndex, (unsigned long)totalMessages, timestamp);
            // Modern: 'from_me' is likely an integer 0/1

            BOOL fromMe = [[amsg objectForKey:@"from_me"] intValue] == 1;

            // NSDate *timestamp = [self convertAndroidTimestamp:[amsg objectForKey:@"timestamp"]];
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


- (void) saveCoreData {
    NSError *error = nil;
    if ([self.moc save:&error] == NO) {
        NSAssert(NO, @"Error saving context: %@\n%@", [error localizedDescription], [error userInfo]);
    }
}

@end
