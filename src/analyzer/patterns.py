"""
Predefined patterns for vaccine bias remorse analysis
"""

REMORSE_PATTERNS = {
    'admission': [
        r'i (?:was|have been) wrong',
        r'changed my (?:mind|opinion|stance|view)',
        r'i regret',
        r'(?:now )?(?:i )?realize',
        r'used to (?:think|believe)',
        r'i admit',
        r'wish i had',
        r'should have listened',
        r'(?:now )?understand',
        r'take back what i said'
    ],
    
    'previous_anti_vax': [
        r'refused (?:to get|getting) (?:the )?(?:vaccine|vax|shot)',
        r'(?:was )?against (?:the )?(?:vaccine|vax|shot)',
        r'thought (?:covid|it) was fake',
        r'conspiracy',
        r'didn\'t trust (?:the )?(?:vaccine|science|doctors)',
        r'wouldn\'t get (?:the )?(?:vaccine|vax|shot)',
        r'(?:vaccine|covid) hoax',
        r'experimental gene therapy',
        r'resisted (?:the )?(?:vaccine|vax|shot)'
    ],
    
    'catalyst': [
        r'got (?:really )?sick',
        r'got covid',
        r'family member',
        r'friend (?:died|passed)',
        r'(?:was )?hospitalized',
        r'lost (?:my |our )?(?:friend|family|parent|spouse)',
        r'personal experience',
        r'(?:doctor|research) showed'
    ],
    
    'current_pro_vax': [
        r'(?:got|getting) (?:the )?(?:vaccine|vax|shot)',
        r'(?:now )?(?:trust|believe) (?:the )?science',
        r'protect (?:others|community|family)',
        r'follow (?:the )?evidence',
        r'listen to doctors',
        r'science is real',
        r'vaccines work',
        r'changed perspective'
    ]
}

POLITICAL_PATTERNS = {
    'conservative': [
        r'republican',
        r'trump',
        r'conservative',
        r'right(?:-| )wing',
        r'freedom',
        r'liberty',
        r'personal choice',
        r'mandate',
        r'government control',
        r'fox news'
    ],
    'progressive': [
        r'democrat',
        r'biden',
        r'liberal',
        r'left(?:-| )wing',
        r'public health',
        r'community',
        r'science',
        r'collective',
        r'responsibility',
        r'msnbc'
    ]
}