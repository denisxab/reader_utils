import pathlib
from base import get_encoding
from pathlib import Path
from abc import abstractstaticmethod
import re
import typing
from dbfread import DBF
from xlrd import Book, open_workbook
from xlrd.sheet import Sheet
import sys

sys.path.insert(0, Path(__file__).parent.parent.__str__())


# pip install dbfread==2.0.7
# pip install xlrd==1.2.0

class RulesConvert:
    """
    Сборник для конвертации типов из шаблона 

    """
    @staticmethod
    def create_rules(template: str) -> tuple[dict[str, str], str]:
        """
        Создать правила для конвертации данных в указанный тип

        Пример:

        >>> RulesConvert.create_rules('''{REFID:ЛюбойТип},  {COUNT:int}''')
        {'REFID': 'ЛюбойТип', 'COUNT': 'int'}
        """

        return {
            k: t for k, t in
            re.findall('{(?P<key>[^:}]+):?(?P<t>[^}]+)?}', template)
        }, re.sub('({[^}:]+)(:[^}]+)}', '\g<1>}', template)

    @staticmethod
    def convert_from_rules(in_dict: dict[str, typing.Any], rules: dict[str, str]):
        """
        Конвертировать данные в указанный тип по переданному правилу(`rules`)

        Пример:

        >>> RulesConvert.convert_from_rules("{'REFID': '991010000014667.10', 'COUNT': '81439'}",{'REFID': 'int', 'COUNT': 'int'})
        {'REFID': 991010000014667, 'COUNT': 81439}
        """

        return {k:
                eval(f"{t}('''{v}''')")
                if t != 'int'
                else eval(f"{t}(float('''{v}'''))")
                for k, v in in_dict.items() if (t := rules.get(k, None)) != None
                }


class BaseParse:

    @abstractstaticmethod
    def goParse(path: str, template: str, **kwargs) -> typing.Iterator[str]:
        ...

    def __init__(self, path: str):
        ...

    def FieldNames(self) -> list[str]:
        """
        Получить все заголовки
        """
        ...

    def getRows(self) -> typing.Iterator[list[typing.Any]]:
        """
        Итератор по записям, берутся все столбцы
        """
        ...

    def escaSql(text: str) -> str:
        """
        Экранирование текста для SQL
        """
        return text.replace("'", "''")


class XlsxParse(BaseParse):

    def __init__(self, path: str) -> None:
        # Выбрать файл
        self.workbook: Book = open_workbook(path)

    def goParse(self, template: str, ifNone='null', sheet: int = 0, esce_call: typing.Callable = BaseParse.escaSql) -> typing.Iterator[str]:
        """

        Конвертировать данный из XLSX файл в шаблон


        Получить данные из `xlsx` файла по пути `path`. Первая строка этого файла будут считаться заголовками, следующие строки
        будут считаться как обычные данные. Вы можете обращаться к данным через шаблон `template`, в котором нужно указать имя заголовка.


        :param template: Шаблон построения ответа `insert into Таблиц value ({ИмяСтолбца_1},{ИмяСтолбца_N})`
        :param ifNone: Что поставить если ячейка пустая
        :param sheet: Номер листа с которого читать
        :param out_path: Путь к файлу в который нужно записать ответ
        :param esce_call: Функция для экранирование значения 
        -------------------------------------------------------------------------------------------
        for x in XlsxParse(r'Файл.xlsx').goParse('select * from dual where id = {ID}'):
            print(x)
        -------------------------------------------------------------------------------------------

        """

        # Создаем правила для конвертации данных
        rules_convert, template = RulesConvert.create_rules(template)

        # Выбрать страницу
        worksheet: Sheet = self.workbook.sheet_by_index(sheet)
        """
        - worksheet
            - .ncols - Всего не пустых столбцов
            - .nrows - Всего не пустых строчек
            - .name - Имя текущей страницы
            - .cell_value(строка,столбец) - Выбрать ячейку по указанным координатам
        """
        # Список заголовков
        head: list[str] = [worksheet.cell_value(
            0, col) for col in range(worksheet.ncols)]
        # Хранения временного результата {ИмяСтолба:ЗначениеСтолбца}
        tmp: dict[str, str]
        for row in range(1, worksheet.nrows):
            tmp = {
                name: r
                if (r := esce_call(str(worksheet.cell_value(row, col)))) else ifNone
                for name, col in zip(head, range(0, worksheet.ncols))
            }
            try:
                # Конвертация в указанный тип
                tmp = RulesConvert.convert_from_rules(tmp, rules_convert)
                # Формируем ответ на основе переданной шаблонной строки
                yield template.format(**tmp)
            except KeyError as e:
                raise KeyError(f"{e}, Не найден столбец с указаны заголовком")

    def getRows(self, sheet: int = 0) -> typing.Iterator[list[typing.Any]]:
        # Выбрать страницу
        worksheet: Sheet = self.workbook.sheet_by_index(sheet)
        for row in range(1, worksheet.nrows):
            yield row

    def FieldNames(self, sheet=0) -> list[str]:
        """
        Получить все заголовки из XLSX
        """
        # Выбрать страницу
        worksheet: Sheet = self.workbook.sheet_by_index(sheet)
        return [worksheet.cell_value(0, col) for col in range(worksheet.ncols)]


class DbfPasre(BaseParse):

    class Record(object):
        """
        Класс для того чтобы можно было получать значение стобца через точку

        :Использование:

        for record in DBF(Путь, encoding=get_encoding(Путь), recfactory=Record):
            record.ИмяСтолбца
        """

        def __init__(self, items):
            for (name, value) in items:
                setattr(self, name, value)

    def __init__(self, path: str):
        """
        :param path: Путь к файлу DBF

        -- Загрузить всю базу в ОЗУ
        self.dbf.load()
        self.dbf.records
        """
        self.dbf = DBF(path, encoding=get_encoding(path))

    def goParse(self, template: str, esce_call: typing.Callable = BaseParse.escaSql) -> typing.Iterator[str]:
        """
        Чтение DBF файлов

        Получить данные из `dbf` файла по пути `path`.Вы можете обращаться к данным через шаблон `template`, в котором нужно указать имя заголовка.

        :param template: Шаблон построения ответа `insert into Таблиц value ({ИмяСтолбца_1},{ИмяСтолбца_N})`
        :param esce_call: Функция для экранирование значения 

        ----------------------------------------------------------------------------------
        for x in DbfPasre.goParse(r'SPRAV_CC_DIRECTIONS.DBF', 'select * from dual where id = {ID_D_GROUP} ref = {CNT_MIN}'):
            print(x)
        ----------------------------------------------------------------------------------
        """
        # Хранения временного результата {ИмяСтолба:ЗначениеСтолбца}
        tmp: dict[str, str]
        # Обрабатываем DBF файл по строчно
        for record in self.dbf:
            # Работа со строками
            tmp = {k: esce_call(str(v)) for k, v in dict(record).items()}
            try:
                # Формируем ответ на основе переданной шаблонной строки
                yield template.format(**tmp)
            except KeyError as e:
                raise KeyError(f"{e}, Не найден столбец с указаны заголовком")

    def getRows(self) -> typing.Iterator[list[typing.Any]]:
        """
        Итератор по записям, берутся все столбцы
        """
        # Обрабатываем DBF файл по строчно
        for record in self.dbf:
            yield record

    def FieldNames(self) -> list[str]:
        """
        Получить все заголовки из DBF
        """
        return self.dbf.field_names

    def goIn(self, field: str, search_set: set[str]) -> bool | set[str]:
        """
        Узнать есть ли вхождение множества `search_set` в столбце `field` в файле `dbf`
        """
        # Получаем все записи указанного столбца из `dbf` файла
        dbf_set: set[str] = set(record[field] for record in self.dbf)
        res = dbf_set.intersection(search_set)
        return res if res else False


if __name__ == '__main__':

    # res = Path('./res.txt')
    # tmp = []
    # xlsxPath = '/media/denis/dd19b13d-bd85-46bb-8db9-5b8f6cf7a825/РАБОТА/big_task/dicinfo.xlsx'
    # for x in XlsxParse(xlsxPath).goParse(
    #     '''"{REFID:int}",'''
    # ):
    #     tmp.append(x)
    #     print(f'{x[:155]}...\n', end='')
    # res.write_text(''.join(tmp))

    obj = XlsxParse(
        r'/media/denis/dd19b13d-bd85-46bb-8db9-5b8f6cf7a825/Dowload/1.2.643.5.1.13.13.11.1477_4.xlsx')
    # print(dbf.FieldNames())
    # print(dbf.goIn('ID_D_GROUP', {-999, 11, 32, 23}))


    # """update dicinfo set EXTCODE={Код:int} where refid=-20222022 and DICNAME='{Наименование}';"""
    res=[]
    for x in obj.goParse("""{Наименование},"""):
        print(x)
        res.append(x)
    (pathlib.Path(__file__).parent/'res.txt').write_text('\n'.join(res))
    