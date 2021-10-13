import subprocess
import sys
import spacy


def install(package):
    subprocess.check_call([sys.executable, "-m", "pip", "install", package])


def install_models():
    # install the models
    print('installing en_ner_jnlpba_md ....')
    # install('https://s3-us-west-2.amazonaws.com/ai2-s2-scispacy/releases/v0.2.5/en_ner_jnlpba_md-0.2.5.tar.gz')
    install('https://s3-us-west-2.amazonaws.com/ai2-s2-scispacy/releases/v0.3.0/en_ner_jnlpba_md-0.3.0.tar.gz')
    # install('https://s3-us-west-2.amazonaws.com/ai2-s2-scispacy/releases/v0.4.0/en_ner_jnlpba_md-0.4.0.tar.gz')
    print('installing en_ner_bc5cdr_md ....')
    # install('https://s3-us-west-2.amazonaws.com/ai2-s2-scispacy/releases/v0.2.5/en_ner_bc5cdr_md-0.2.5.tar.gz')
    install('https://s3-us-west-2.amazonaws.com/ai2-s2-scispacy/releases/v0.3.0/en_ner_bc5cdr_md-0.3.0.tar.gz')
    # install('https://s3-us-west-2.amazonaws.com/ai2-s2-scispacy/releases/v0.4.0/en_ner_bc5cdr_md-0.4.0.tar.gz')


def load_models(model_name):
    print('** loading ' + model_name + ' ....')
    model = spacy.load(model_name)
    # model.add_pipe('scispacy_linker', config={'resolve_abbreviations': True, 'linker_name': 'umls',
    #                                                'threshold': 0.75, 'max_entities_per_mention': 3})
    return model
