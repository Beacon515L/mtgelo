<?php
//Parse the JSON
$atomicCards = json_decode(file_get_contents(realpath(dirname(__FILE__)) ."/AtomicCards.json"));
//Identify the version and date of this data
$atomicCardsMeta = $atomicCards->meta;
$date = $atomicCardsMeta->date;
$version = $atomicCardsMeta->version;
//var_dump(array_keys(get_object_vars($atomicCards->data)));
//var_dump(array_values(get_object_vars($atomicCards->data))[0][0]);
//die();

//Connect to the database
require_once(realpath(dirname(__FILE__))."/../config.php");
$db = db_connect();

//Verify if this data is already applied; if so, quit
$versionCheckResult = $db->query("
    SELECT date_applied 
    FROM card_meta
    WHERE version = '".$db->real_escape_string($version)."'
    AND `date` = '".$db->real_escape_string($date)."'
    LIMIT 1 ORDER BY `date` DESC");

$result = ($versionCheckResult)?$versionCheckResult->fetch_row()[0] ?? null:null;

if($result) {
    echo "Data already imported on ".$result->value.".".PHP_EOL;
    exit;
}

//Identify what cards are new and what cards aren't
$newCards = [];
$updateCards = [];
try {
//From this point we need data consistency.  Begin the transaction.
$db->begin_transaction();

$cardCheckResult = $db->query("
    SELECT id, name
    FROM card
");

/** @var stdClass $row */
while ($cardCheckResult && ($row = $cardCheckResult->fetch_assoc()) != false){
    $updateCards[$row["id"]] = new stdClass();
    $updateCards[$row["id"]]->name = $row["name"];
}
$cardsToIterate = get_object_vars($atomicCards->data);
$cardsToUpdate = 0;
foreach($cardsToIterate as $card){
    $updateCardId = null; $card = $card[0];
    foreach ($updateCards as $id => $updateCard){
        if($updateCard->name == $card->name){
            $updateCardId = $id; break;
        }
    }
        if($updateCardId !== null){
            $updateCards[$updateCardId] = $card; $cardsToUpdate++;
        }
        else {
            $newCards[] = $card;
        }
    echo "Loaded ".$cardsToUpdate."/".count($updateCards)." cards to update, ".count($newCards)." to insert.".PHP_EOL;
        continue;
}
echo "Ready to import. ".count($updateCards)." cards to update, ".count($newCards)." to insert.".PHP_EOL;
//At this point we can now process each of the cards.
$updated = 0; $created = 0;

//Update the existing cards
foreach ($updateCards as $id => $updateCard){
    echo "Updating ".$updated++." of ".count($updateCards).PHP_EOL;
    addCard($db,$updateCard,$id);
}

//Add the new cards
foreach ($newCards as $newCard){
    echo "Creating ".$created++." of ".count($newCards).PHP_EOL;
    addCard($db,$newCard);
}
$db->query("INSERT INTO card_meta (date, version) VALUES ('".$db->real_escape_string($date)."','".$db->real_escape_string($version)."')");
if(!$db->commit()) throw new Exception($db->error);
echo "Successfully completed!".PHP_EOL;

$count_result = $db->query("SELECT COUNT(1) FROM card");
var_export($count_result->fetch_row()[0] ?? null);
}
catch (Exception $e){
    $db->rollback();
    var_dump($e);
}
$db->close();
function addCard($db,$card, $id = null){
    echo "Parsing: ".$card->name;
    //Schematically conform the object
    $card->name = !(empty($card->name))?$card->name:"";
    $card->colourIdentity = !(empty($card->colorIdentity))?implode($card->colorIdentity):null;
    $card->colourIndicator = !(empty($card->colorIndicator))?implode($card->colorIndicator):null;
    $card->colours = !(empty($card->colors))?implode($card->colors):null;
    $card->convertedManaCost = !(empty($cards->convertedManaCost))?$cards->convertedManaCost:null;
    $card->edhrecRank = !(empty($card->edhrecRank))?$card->edhrecRank:null;
    $card->faceConvertedManaCost = !(empty($card->faceConvertedManaCost))?$card->faceConvertedManaCost:null;
    $card->faceName = !(empty($card->faceName))?$card->faceName:null;
    $card->hand = !(empty($card->hand))?(intval(substr($card->hand,1))*(substr($card->hand,0,1)=="+")?1:-1):null;
    $card->hasAlternativeDeckLimit = !(empty($card->hasAlternativeDeckLimit))?$card->hasAlternativeDeckLimit:null;
    $card->isReserved = !(empty($card->isReserved))?$card->isReserved:null;
    $card->life = !(empty($card->life))?(intval(substr($card->life,1))*(substr($card->life,0,1)=="+")?1:-1):null;
    $card->loyalty = !(empty($card->loyalty))?$card->loyalty:null;
    $card->manaCost = !(empty($card->manaCost))?$card->manaCost:null;
    $card->power = !(empty($card->power))?$card->power:null;
    $card->side = !(empty($card->side))?$card->side:null;
    $card->text = !(empty($card->text))?$card->text:null;
    $card->toughness = !(empty($card->toughness))?$card->toughness:null;
    $card->type = !(empty($card->type))?$card->type:null;
    //var_export($card); die();
    echo " SCH";
    if($id!==null) {
        echo " SUM";
        //Delete all the existing associations for this card
        $db->query("DELETE FROM card_keyword WHERE card_id ='" . $db->real_escape_string($id) . "'");
        $db->query("DELETE FROM card_layout WHERE card_id ='" . $db->real_escape_string($id) . "'");
        $db->query("DELETE FROM card_leadershipskills WHERE card_id ='" . $db->real_escape_string($id) . "'");
        $db->query("DELETE FROM card_legalities WHERE card_id ='" . $db->real_escape_string($id) . "'");
        $db->query("DELETE FROM card_sets WHERE card_id ='" . $db->real_escape_string($id) . "'");
        $db->query("DELETE FROM card_type WHERE card_id ='" . $db->real_escape_string($id) . "'");
        $db->query("DELETE FROM identifier WHERE card_id ='" . $db->real_escape_string($id) . "'");
        $db->query("DELETE FROM rulings WHERE card_id ='" . $db->real_escape_string($id) . "'");
        echo " DEL";
        $stmt = $db->stmt_init();
        $stmt->prepare(
            "UPDATE card
            SET `name`                    = ?,
                colourIdentity          = ?,
                colorIndicator         = ?,
                colours                 = ?,
                convertedManaCost       = ?,
                edhrecRank              = ?,
                faceConvertedManaCost   = ?,
                faceName                = ?,
                hand                    = ?,
                hasAlternativeDeckLimit = ?,
                isReserved              = ?,
                life                    = ?,
                loyalty                 = ?,
                manaCost                = ?,
                power                   = ?,
                side                    = ?,
                text                    = ?,
                toughness               = ?,
                `type`                    = ?
            WHERE id = ?"
        ) or throw new Exception($db->error);;
        $stmt->bind_param("ssssdidsiiiisssssssi",
            $card->name, $card->colourIdentity, $card->colourIndicator, $card->colours,
            $card->convertedManaCost, $card->edhrecRank, $card->faceConvertedManaCost, $card->faceName,
            $card->hand, $card->hasAlternativeDeckLimit, $card->isReserved, $card->life,
            $card->loyalty, $card->manaCost, $card->power, $card->side, $card->text, $card->toughness,
            $card->type, $id
        ) or throw new Exception($db->error);;
        $stmt->execute() or throw new Exception($db->error);
        /*$db->query("
            UPDATE card
            SET name                    = '".$db->real_escape_string($card->name)."',
                colourIdentity          = '".$db->real_escape_string($card->colourIdentity)."',
                colorIndicator         = '".$db->real_escape_string($card->colourIndicator)."',
                colours                 = '".$db->real_escape_string($card->colours)."',
                convertedManaCost       = ".$db->real_escape_string($card->convertedManaCost).",
                edhrecRank              = ".$db->real_escape_string($card->ehdrecRank).",
                faceConvertedManaCost   = ".$db->real_escape_string($card->faceConvertedManaCost).",
                faceName                = '".$db->real_escape_string($card->faceName)."',
                hand                    = ".$db->real_escape_string($card->hand).",
                hasAlternativeDeckLimit = ".$db->real_escape_string($card->hasAlternativeDeckLimit?1:0).",
                isReserved              = ".$db->real_escape_string($card->isReserved?1:0).",
                life                    = ".$db->real_escape_string($card->life).",
                loyalty                 = '".$db->real_escape_string($card->loyalty)."',
                manaCost                = '".$db->real_escape_string($card->manaCost)."',
                power                   = '".$db->real_escape_string($card->power)."',
                side                    = '".$db->real_escape_string($card->side)."',
                text                    = '".$db->real_escape_string($card->text)."',
                toughness               = '".$db->real_escape_string($card->toughness)."',
                type                    = '".$db->real_escape_string($card->type)."'
            WHERE id = ".$db->real_escape_string($id)
        ) or throw new Exception($db->error);*/
        echo " UPD";
    }
    else {
        echo " NON";
        $stmt = $db->stmt_init();
        $stmt->prepare("
        INSERT INTO card (
                `name`,
                colourIdentity,
                colorIndicator,
                colours,
                convertedManaCost,
                edhrecRank,
                faceConvertedManaCost,
                faceName,
                hand,
                hasAlternativeDeckLimit,
                isReserved,
                life,
                loyalty,
                manaCost,
                power,
                side,
                text,
                toughness,
                `type`
        )
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    ") or throw new Exception($db->error);;
        $stmt->bind_param("ssssdidsiiiisssssss",
            $card->name, $card->colourIdentity, $card->colourIndicator, $card->colours,
            $card->convertedManaCost, $card->edhrecRank, $card->faceConvertedManaCost, $card->faceName,
            $card->hand, $card->hasAlternativeDeckLimit, $card->isReserved, $card->life,
            $card->loyalty, $card->manaCost, $card->power, $card->side, $card->text, $card->toughness,
            $card->type
        ) or throw new Exception($db->error);;
        $stmt->execute() or throw new Exception($db->error);
    /*$db->query("
        INSERT INTO card (
                name,
                colourIdentity,
                colorIndicator,
                colours,
                convertedManaCost,
                edhrecRank,
                faceConvertedManaCost,
                faceName,
                hand,
                hasAlternativeDeckLimit,
                isReserved,
                life,
                loyalty,
                manaCost,
                power,
                side,
                text,
                toughness,
                type
        )
        VALUES (
            '".$db->real_escape_string($card->name)."',
            '".$db->real_escape_string($card->colourIdentity)."',
             '".$db->real_escape_string($card->colourIndicator)."',
             '".$db->real_escape_string($card->colours)."',
             ".$db->real_escape_string($card->convertedManaCost).",
             ".$db->real_escape_string($card->edhrecRank).",
             ".$db->real_escape_string($card->faceConvertedManaCost).",
             '".$db->real_escape_string($card->faceName)."',
             ".$db->real_escape_string($card->hand).",
             ".$db->real_escape_string($card->hasAlternativeDeckLimit?1:0).",
             ".$db->real_escape_string($card->isReserved?1:0).",
             ".$db->real_escape_string($card->life).",
             '".$db->real_escape_string($card->loyalty)."',
             '".$db->real_escape_string($card->manaCost)."',
             '".$db->real_escape_string($card->power)."',
             '".$db->real_escape_string($card->side)."',
             '".$db->real_escape_string($card->text)."',
             '".$db->real_escape_string($card->toughness)."',
             '".$db->real_escape_string($card->type)."'
        )
    ") or throw new Exception($db->error);*/
    $id = $db->insert_id;
    echo " ID".$id;
    }

    //Keywords
    foreach(!empty($card->keywords)?$card->keywords:[] as $keyword){
        assignAttribute($db,"keywords","card_keyword","value","keyword_id",$id,$keyword);
    }
    echo " KWD";

    //Layouts
    foreach(!empty($card->layouts)?$card->layouts:[] as $layout){
        assignAttribute($db,"layouts","card_layout","name","layout_id",$id,$layout);
    }
    echo " LYT";
    
    //Leadership skills
    foreach(!empty($card->leadershipSkills)?$card->leadershipSkills:[] as $leadershipSkill){
        assignAttribute($db,"leadershipskills","card_leadershipskills","name","leadershipSkills_id",$id,$leadershipSkill);
    }
    echo " LSS";

    //Legalities
    foreach(!empty($card->legalities)?$card->legalities:[] as $legality){
        $db->query("INSERT INTO legalities (card_id, format_name) VALUES (".$id.",'".$db->real_escape_string($legality)."')");
    }
    echo " LAW";
    //Sets
    foreach(!empty($card->printings)?$card->printings:[] as $set){
        assignAttribute($db,"sets","card_sets","name","set_id",$id,$set);
    }
    echo " SET";

    //Types
    foreach(!empty($card->subtypes)?$card->subtypes:[] as $subtype){
        assignAttribute($db,"types","card_types","name","type_id",$id,$subtype,null,true);
    }
    echo " SUB";
    foreach(!empty($card->supertypes)?$card->supertypes:[] as $supertype){
        assignAttribute($db,"types","card_types","name","type_id",$id,$supertype,true);
    }
    echo " SUP";
    foreach(!empty($card->types)?$card->types:[] as $type){
        assignAttribute($db,"types","card_types","name","type_id",$id,$type);
    }
    echo " TYP";

    //Identifiers
    $identifierKeys = [
        "scryfallOracle" => "scryfallOracleId"
    ];

    foreach($identifierKeys as $identifierKey => $identifierField){
        assignAttribute($db,"identifierTypes","identifiers","name","identifierType_id",$id,$identifierKey, null, null, $card->identifiers->{$identifierField});
    }
    //Purchase Identifiers
    /*$purchaseIdentifierKeys = [
        "cardKingdom" => "cardKingdom",
        "cardKingdomFoil" => "cardKingdomFoil",
        "cardmarket" => "cardmarket",
        "tcgplayer" => "tcgplayer"
    ];*/
    $purchaseIdentifierKeys = !empty($card->purchaseUrls)?array_keys(get_object_vars($card->purchaseUrls)):[];

    foreach($purchaseIdentifierKeys as $purchaseIdentifierKey){
        assignAttribute($db,"identifierTypes","identifiers","name","identifierType_id",$id,$purchaseIdentifierKey, null, null, get_object_Vars($card->purchaseUrls)[$purchaseIdentifierKey]);
    }
    echo " PUR";

    echo " TYP";
    //Rulings
    foreach (!empty($card->rulings)?$card->rulings:[] as $ruling){
        $db->query("INSERT INTO rulings (card_id, date, text) VALUES (".$id.",\'".$ruling->date."','".$db->real_escape_string($ruling->text)."')");
    }
    echo " REX".PHP_EOL;
}

function assignAttribute($db, $tableName, $assignTableName, $valueColName, $assignColName, $id, $value, $superType = null, $subType = null, $identifierValue = null){
    $existenceCheckResult = $db->query("SELECT id FROM ".$tableName." WHERE ".$valueColName." = '".$db->real_escape_string($value)."'");
    $attributeId = ($existenceCheckResult)?$existenceCheckResult->fetch_row()[0] ?? null:null;
    if($attributeId===null){
        $db->query("INSERT INTO ".$tableName." (".$valueColName.") VALUES ('".$db->real_escape_string($value)."')");
        $attributeId = $db->insert_id;
    }
    if($subType == true || $superType == true){
        $db->query("UPDATE type SET isSubtype = ".($subType?1:"isSubtype").", isSupertype = ".($superType?1:"isSupertype")." WHERE id = ".$attributeId );
    }

    $db->query("INSERT INTO ".$assignTableName." (card_id, ".$assignColName.") VALUES (".$id.",".$attributeId.")");
}