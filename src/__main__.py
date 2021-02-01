from application.repetitive_execution import RepetitiveExecution
from dependency_injection.containers import ApplicationProviders


def main():
    ApplicationProviders.application().run()


if __name__ == "__main__":
    main()
