package overview

import (
	"log"
	"strings"
	"sync"
)

type SPAMFILTER struct {
	mux sync.RWMutex
	/*
	FILE_BAD_FROM_MATCH string  // filter.from.match
	FILE_BAD_FROM_CONTAINS string // filter.from.contains
	FILE_BAD_FROM_SUFFIX string // filter.from.suffix
	FILE_BAD_SUBJ_CONTAINS string // filter.subject.contains
	SP_BAD_FROM_MATCH []string
	SP_BAD_FROM_CONTAINS []string
	SP_BAD_SUBJ_CONTAINS []string
	*/
}


var (
	/* subject
	indonesia; kamboja; macau; 777; 888; casino; slots; sports; login;
	premium; slot freebet; badakslot; gigabet; daftar; situs; toto;
	lotto; maxwin; max win; jackpot; jackpot games; hyperspins;
	anti blokir; tanpa deposit; terbaru; terpercaya; gacor; klikfifa;
	bandar; sinar; ufa356; ufa666; u200d; betfli; populer; wahana
	*/

	/*
	Plus some of the most frequent occurring emojis in the subject line.
	Useful the including of these figures 303, 55, 77, 88, 99 in both lines.
	(Esp. 88 seems to be part of various Indonesian and Malaysian names for gaming / slot machine schemes.)
	*/

	/* NNTP-Posting-Host
	"103.138.1"
	"2a0e:a942:"
	"2a0d:5600:"
	"47.185.2"
	"45.114."
	"96.9."    (in Phnom Penh)
	"103.55.3" (in Djakarta)
	*/

	BAD_FROM_MATCH = []string{
		"FBInCIAnNSATerroristSlayer <FBInCIAnNSATerroristSlayer@yahoo.com",
	}

	BAD_FROM_CONTAINS = []string{
		"root@127.0.0.1",
		"_@_.__",
		"dmbtimesinc@aol.com",
		"@jbinup",
		"CPP-Gebruiker",
		"HeartDoc Andrew",
		"forger@",
		"forgeries@",
		"forgery@",
		"forgers@",
		"forger@",
		"paypal",
		"nforystek@sosouix.net",
		"kill@llspammers.dead",
		"front@556184.net",
		"disciple@T3WiJ.com",
		"loandbehold8434@hotmail.com",
		"SummersTrees@hotmail.com",
		"e447560@rppkn.com",
		"xyz91987@gmail.com",
		"grigiox4@gmail.com",
		"r.c.bates@btinternet.com",
		"@getfucked.com",
		"indiabusinesszone954@gmail.com",
		"habicahidi@gmail.com",
		"l4mttdnwdm@gmail.com",
		"fkhall@gmail.com",
		"00@derbyshire.kooks.out",
		"laughing@u.kook",
		"jovuli.elbahja@gmail.com",
		"thrinaxodon.fan.club512@gmail.com",
		"FNVWe@altusenetkooks.xxx",
		"maxcell9999@gmail.com",
		"@fuckhoward.net",
		"dudumacudu@mail.com",
		"nad318b404@gmail.invalid",
		"rahul.rwaltz@gmail.com",
		"enometh@meer.net",
		"pramod@confluxsystems.com",
		"nomesh.usithr@gmail.com",
		"kesava1.conflux@gmail.com",
		"xyz91987@google.com",
		"killvirus@coronavirus.com",
		"@NoReply.Invalid.com",
		"@RubyRidge.COM",
		"@XXX999.net",
		"@670iybn.com",
		"@dhp.com",
		"jd6471836@gmail.com",
		"theusenet@mail.com",
		"haya0626@mxb.meshnet.or.jp",
		"noreply@breaka.net",
		"marwa.kotb2@mediu.ws",
		"ladypilot7@gmail.com",
		"discounts@iphone",
		"ast.ahad@gmail.com",
		"jerrycalzado@gmx.com",
		"pippobattipaglia@yahoo.es",
		"ginobusciarello@outlook.com",
		"thehighestgod@gmail.com",
		"gburnore@databasix.com",
		"arcturianone@earthlink.net",
		"nostalkingme@rocketmail.com",
		"nanaestalkers@yahoo.ca",
		"stopstalking@gmx.com",
		"daniellamirande45@gmail.com",
		"nomesh.usithr@gmail.com",
		"Exoticpsychstore",
		"exoticpsychstore@gmail.com",
		"FBInCIAnNSATerroristSlayer",
		"penner@dfsadfsdfsdf.de",
		"FreeBsd@StoleMySoul.com",
		"exposed4@all.2c",
		"nforystek@outlook.com",
		"@bigfoot.com",
		"newsserver@freedyn.net",
		"danijela_milosevic@hotmail.com",
		"ooxfxs@hornysex.com",
		"make@money.com",
		"pma4jobs",
		"anabana1233@hotmail.com",
		"crisisacris@terra.es",
		"verdad@mac.com",
	}

	BAD_FROM_SUFFIX = []string{
	}

	BAD_SUBJ_SUFFIX = []string{
		"$$$",
		"!!!",
	}

	BAD_SUBJ_PREFIX = []string{
		"AD: ~~**",
		"TURN $",
		"Buy ",
		" Buy ",
		"•••",
		">>>",
		"~~>",
		"~~~",
		"~~*",
		"$$$",
		"!!!",
		"ï¿½ï",
	}

	BAD_SUBJ_MATCH = []string{
		".",
		",",
		";",
		"-",
		"+",
	}

	BAD_SUBJ_CONTAINS = []string{
		"paypal.txt",
		"paypal.doc",
		"paypal.rtf",
		" yenc (",
		"全国47",
		"します。",
		"すよ!!",
		"代限定",
		"【登録無",
		"财务自由",
		"【確実です】",
		"個已證實可行的增加收入來源方式",
		"收入流",
		"40歳",
		".....",
		"how to turn",
		"extra money",
		"Make a lot",
		"Make up to",
		"Make Money",
		"Money make",
		" of money ",
		"money now",
		"easy Money",
		"make some money",
		"I found this great little site",
		"FREE PHONESEX",
		"VIAGRA",
		"V1AGRA",
		"PENIS",
		"P3NIS",
		"P3N1S",
		"PeN1S",
		"trip to disney",
		"AN0NYMOUS ",
		"best bargains",
		"Anonymity &",
		"Free usenet access",
		"Free Aonymous",
		"Free, Aonymous",
		"AN0N EMAIL",
		"free full service",
		"lifetime access",
		"free, anon",
		"free anon",
		"An0nYm0uS ",
		"FREE An0n",
		"FREE, An0n",
		"TruthNBP",
		"The TruthNBP Unregistered",
		"benchsales",
		"chocolate bars",
		"buy pills online",
		"Exoticpsychstore",
		"Cocaine Online",
		"Cocaine Powder",
		"Buy Powder Cocaine",
		"Colombian Cocaine",
		"pens for sale",
		"for sale online",
		"Psychedelics For Sale",
		"Buy Amphetamine",
		"Buy Morphin",
		"Buy Ritalin",
		"buy Xanax",
		"Buy Jwh",
		"Buy Alprazolam",
		"Buy Counterfeit",
		"Buy MDMA",
		"Buy Acid",
		"Buy LSD",
		"Cocaine",
		"Ibogaine",
		"psilocybin",
		"Mescaline",
		"PSYCHEDELICS",
		"Roxicodone",
		"Oxycontin",
		"Vicodin",
		"Nembutal",
		"Ketamine",
		"adderal",
		"hydromorphone",
		"hydrocodone",
		"OxyContin",
		"Oxycodone",
		"Buy Oxycodone",
		"Vyvanse",
		"Cheap Ink Cartridges",
		"primepsychedelicz",
		"NEW PHONESEX SITE",
		"JESUS IS LORD",
		"Free Dating",
		"promotion of your Business",
		"David Ritz's",
		"ONLINE PILLS",
		"warez 4 sale",
		"PEDERASTA INCULA BAMBINI",
		"PEDOFILO INCULA BAMBINI",
		"save LOTS of CASH",
		"a new trick ... ",
		"new trick: ",
		"mail2news: ",
		"Another Ride: ",
		"Ride, Captain Ride: ",
		"Totalitarian dictatorships suck and suck profoundly",
		"Russ Allbery is a godfather of brainwashing",
		"Free usenet of totalitarian dicators and fascists",
		"is a totalitarian dictator of usenet",
		"is a self-admitted fascist and censor",
		"Founding father of AI Marvin Minsky is in delusion",
		"is intolerant blood boiling idiot",
		"is harboring totalitarians and fascists",
		"Moderation is evil totalitarian and fascist concept",
		"Stop Usenet Censorship and Totalitarianism",
		"Totalitarians and fascists do not like open and FREE servers",
		"Neal Warren",
		"Greg Hall",
		"Andrew Giert",
		"Al madinah international university",
		"SCHIFOSO BASTARDO NEOPIDUISTA",
		"MANDANTE DI OMICIDI E MEGALAVA CASH MAFIOSO",
		"coronavirus",
		"randomly punched in the head",
		"mushroom",
		"shrooms",
		"Dr Shantanu Panigrahi",
		"Hot Gay",
		"Adult Dating",
		"Maximum male",
		"male enhancement",
		"Russian Language For All !",
		"EARN really in the web",
		"PUSSY AND HARD DICKS FOR FREE",
		"adult sex",
		"movie download",
		"movie online",
	}
)

func (s *SPAMFILTER) Spamfilter(input string, spamtype string, msgid string) bool {
	if input == "" {
		log.Printf("Error OV spamfilter input=nil")
		return true
	}
	switch spamtype {
	case "subj":
		for _, bad := range BAD_SUBJ_MATCH {
			if input == bad {
				//log.Printf("SPAMFILTER BAD_SUBJ_MATCH msgid='%s' subj='%s' bad='%s'", msgid, input, bad)
				return true
			}
		}
		for _, bad := range BAD_SUBJ_PREFIX {
			if strings.HasPrefix(strings.ToLower(input), strings.ToLower(bad)) {
				//log.Printf("SPAMFILTER BAD_SUBJ_PREFIX msgid='%s' subj='%s' bad='%s'", msgid, input, bad)
				return true
			}
		}
		for _, bad := range BAD_SUBJ_SUFFIX {
			if strings.HasSuffix(strings.ToLower(input), strings.ToLower(bad)) {
				//log.Printf("SPAMFILTER BAD_SUBJ_SUFFIX msgid='%s' subj='%s' bad='%s'", msgid, input, bad)
				return true
			}
		}
		for _, bad := range BAD_SUBJ_CONTAINS {
			if strings.Contains(strings.ToLower(input), strings.ToLower(bad)) {
				//log.Printf("SPAMFILTER BAD_SUBJ_CONTAINS msgid='%s' subj='%s' bad='%s'", msgid, input, bad)
				return true
			}
		}
	case "from":
		for _, bad := range BAD_FROM_MATCH {
			if strings.ToLower(bad) == strings.ToLower(input) {
				//log.Printf("SPAMFILTER BAD_FROM_MATCH msgid='%s' from='%s' bad='%s'", msgid, input, bad)
				return true
			}
		}
		for _, bad := range BAD_FROM_SUFFIX {
			if strings.ToLower(bad) == strings.ToLower(input) {
				//log.Printf("SPAMFILTER BAD_FROM_SUFFIX msgid='%s' from='%s' bad='%s'", msgid, input, bad)
				return true
			}
		}
		for _, bad := range BAD_FROM_CONTAINS {
			if strings.Contains(strings.ToLower(input), strings.ToLower(bad)) {
				//log.Printf("SPAMFILTER BAD_FROM_CONTAINS msgid='%s' from='%s' bad='%s'", msgid, input, bad)
				return true
			}
		}
	}
	return false
} // end func spamfilter
